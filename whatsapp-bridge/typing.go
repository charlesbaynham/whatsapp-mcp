package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	// WhatsApp shows "typing…" for only a few seconds per chatstate node and
	// then drops it, so a hold that is meant to last while an agent thinks has
	// to keep repeating the composing node the way a real client does.
	typingRefreshInterval = 5 * time.Second

	// How long a hold lasts if nothing renews or rescinds it. Whoever is
	// typing on our behalf is an ephemeral agent session that can die without
	// warning, so every hold expires on its own: a "typing…" that never stops
	// is worse than one that stops early, because it is a promise of a reply
	// that is never coming.
	defaultTypingTTL = 5 * time.Minute
	maxTypingTTL     = 30 * time.Minute

	presenceTimeout = 10 * time.Second
)

// chatPresenceSender is the part of *whatsmeow.Client the typing manager
// needs, so its tests do not need a WhatsApp connection.
type chatPresenceSender interface {
	SendPresence(ctx context.Context, state types.Presence) error
	SendChatPresence(ctx context.Context, jid types.JID, state types.ChatPresence, media types.ChatPresenceMedia) error
}

// TypingStatus is one live hold, as the REST API reports it.
type TypingStatus struct {
	ChatJID   string    `json:"chat_jid"`
	StartedAt time.Time `json:"started_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type typingHold struct {
	started time.Time
	expires time.Time
	stop    chan struct{}
}

// typingManager keeps "typing…" showing in a chat for as long as someone is
// working on a reply to it.
//
// One hold per chat: starting a hold that already exists extends it rather
// than stacking a second refresher. A hold ends when it is stopped
// explicitly (the reply was sent, or the decision was not to reply), or when
// its deadline passes — never silently, because both endings send the paused
// chatstate that takes the indicator away.
type typingManager struct {
	client  chatPresenceSender
	logger  waLog.Logger
	refresh time.Duration
	ttl     time.Duration
	// enabled false turns the whole feature off: Start becomes a no-op and
	// nothing is ever sent (WHATSAPP_BRIDGE_TYPING=off).
	enabled bool
	now     func() time.Time

	mu    sync.Mutex
	holds map[string]*typingHold

	presenceMu sync.Mutex
	online     bool
}

func newTypingManager(client chatPresenceSender, logger waLog.Logger) *typingManager {
	return &typingManager{
		client:  client,
		logger:  logger,
		refresh: typingRefreshInterval,
		ttl:     defaultTypingTTL,
		enabled: true,
		now:     time.Now,
		holds:   make(map[string]*typingHold),
	}
}

// newTypingManagerFromEnv reads WHATSAPP_BRIDGE_TYPING (off/0/false disables
// the indicator entirely) and WHATSAPP_BRIDGE_TYPING_TTL_SECONDS (how long an
// unrenewed hold lasts).
func newTypingManagerFromEnv(client chatPresenceSender, logger waLog.Logger) *typingManager {
	m := newTypingManager(client, logger)
	switch os.Getenv("WHATSAPP_BRIDGE_TYPING") {
	case "off", "0", "false", "no":
		m.enabled = false
	}
	if raw := os.Getenv("WHATSAPP_BRIDGE_TYPING_TTL_SECONDS"); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
			m.ttl = clampTypingTTL(time.Duration(secs) * time.Second)
		} else {
			logger.Warnf("typing: ignoring WHATSAPP_BRIDGE_TYPING_TTL_SECONDS=%q", raw)
		}
	}
	return m
}

func clampTypingTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return defaultTypingTTL
	}
	if ttl > maxTypingTTL {
		return maxTypingTTL
	}
	return ttl
}

// Start shows "typing…" in a chat now and keeps it showing until ttl runs
// out, Stop is called, or a message goes out to that chat. A zero ttl uses
// the configured default; an existing hold has its deadline moved rather than
// being restarted, so a later caller can only extend what an earlier one
// began.
func (m *typingManager) Start(jid types.JID, ttl time.Duration) (TypingStatus, error) {
	if m == nil || !m.enabled {
		return TypingStatus{}, nil
	}
	if ttl <= 0 {
		ttl = m.ttl
	}
	ttl = clampTypingTTL(ttl)
	key := typingKey(jid)
	now := m.now()

	m.mu.Lock()
	if hold, ok := m.holds[key]; ok {
		if deadline := now.Add(ttl); deadline.After(hold.expires) {
			hold.expires = deadline
		}
		status := TypingStatus{ChatJID: key, StartedAt: hold.started, ExpiresAt: hold.expires}
		m.mu.Unlock()
		// Repeat the chatstate even on an extension: the caller has just told
		// us it is still working, and the last refresh may be seconds old.
		m.compose(jid)
		return status, nil
	}
	hold := &typingHold{started: now, expires: now.Add(ttl), stop: make(chan struct{})}
	m.holds[key] = hold
	m.mu.Unlock()

	if err := m.compose(jid); err != nil {
		// Nothing is showing, so do not leave a refresher running for it.
		m.clear(key)
		return TypingStatus{}, err
	}
	go m.keepComposing(jid, key, hold)
	return TypingStatus{ChatJID: key, StartedAt: hold.started, ExpiresAt: hold.expires}, nil
}

// Stop rescinds the indicator: the paused chatstate goes out immediately and
// the refresher stops. Calling it for a chat with no hold is not an error —
// it still sends paused, so a session that is unsure whether it (or the
// bridge) started one can always clear it.
func (m *typingManager) Stop(jid types.JID) error {
	if m == nil || !m.enabled {
		return nil
	}
	m.clear(typingKey(jid))
	err := m.pause(jid)
	m.settlePresence()
	return err
}

// StopChat is Stop for a chat JID in string form, for the send path, which
// has already resolved the recipient to the JID it really sent to.
func (m *typingManager) StopChat(chatJID string) {
	if m == nil || !m.enabled || chatJID == "" {
		return
	}
	if !m.Active(chatJID) {
		// Nothing is showing: an outgoing message no one was typing for needs
		// no paused node.
		return
	}
	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return
	}
	if err := m.Stop(jid); err != nil {
		m.logger.Warnf("typing: could not clear the indicator for %s after sending: %v", chatJID, err)
	}
}

// Enabled reports whether this bridge shows typing indicators at all.
func (m *typingManager) Enabled() bool {
	return m != nil && m.enabled
}

// Active reports whether a chat currently has a hold.
func (m *typingManager) Active(chatJID string) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.holds[chatJID]
	return ok
}

// List returns every live hold, oldest first.
func (m *typingManager) List() []TypingStatus {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	out := make([]TypingStatus, 0, len(m.holds))
	for key, hold := range m.holds {
		out = append(out, TypingStatus{ChatJID: key, StartedAt: hold.started, ExpiresAt: hold.expires})
	}
	m.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.Before(out[j].StartedAt) })
	return out
}

// WakeForChat is the hook the webhook dispatcher calls when an incoming
// message is about to wake an agent: the guest should see "typing…" from the
// moment their message lands, not from whenever the woken session gets round
// to its first tool call.
func (m *typingManager) WakeForChat(chatJID string) {
	if m == nil || !m.enabled || chatJID == "" {
		return
	}
	jid, err := types.ParseJID(chatJID)
	if err != nil || jid.User == "" {
		return
	}
	if _, err := m.Start(jid, 0); err != nil {
		m.logger.Warnf("typing: could not start the indicator for %s: %v", chatJID, err)
	}
}

// keepComposing re-sends the composing chatstate until the hold is stopped or
// its deadline passes.
func (m *typingManager) keepComposing(jid types.JID, key string, hold *typingHold) {
	ticker := time.NewTicker(m.refresh)
	defer ticker.Stop()
	for {
		select {
		case <-hold.stop:
			return
		case <-ticker.C:
			m.mu.Lock()
			current, ok := m.holds[key]
			expires := time.Time{}
			if ok {
				expires = current.expires
			}
			m.mu.Unlock()
			if !ok || current != hold {
				return
			}
			if !m.now().Before(expires) {
				// Lapsed. Take the indicator away rather than leaving the last
				// composing node to time out on its own.
				m.clear(key)
				if err := m.pause(jid); err != nil {
					m.logger.Warnf("typing: could not clear the lapsed indicator for %s: %v", key, err)
				}
				m.settlePresence()
				return
			}
			if err := m.compose(jid); err != nil {
				m.logger.Warnf("typing: could not refresh the indicator for %s: %v", key, err)
			}
		}
	}
}

// clear drops a chat's hold and stops its refresher. Safe to call for a chat
// that has none.
func (m *typingManager) clear(key string) {
	m.mu.Lock()
	hold, ok := m.holds[key]
	if ok {
		delete(m.holds, key)
	}
	m.mu.Unlock()
	if ok {
		close(hold.stop)
	}
}

func (m *typingManager) compose(jid types.JID) error {
	m.ensureOnline()
	ctx, cancel := context.WithTimeout(context.Background(), presenceTimeout)
	defer cancel()
	return m.client.SendChatPresence(ctx, jid, types.ChatPresenceComposing, types.ChatPresenceMediaText)
}

func (m *typingManager) pause(jid types.JID) error {
	ctx, cancel := context.WithTimeout(context.Background(), presenceTimeout)
	defer cancel()
	return m.client.SendChatPresence(ctx, jid, types.ChatPresencePaused, types.ChatPresenceMediaText)
}

// ensureOnline marks the account available, which WhatsApp requires before it
// will show chat presence from us at all. It affects delivery receipts (that
// is whatsmeow's own behaviour for an available client) but not read state:
// blue ticks still only move through /api/mark-read.
func (m *typingManager) ensureOnline() {
	m.presenceMu.Lock()
	defer m.presenceMu.Unlock()
	if m.online {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), presenceTimeout)
	defer cancel()
	if err := m.client.SendPresence(ctx, types.PresenceAvailable); err != nil {
		// Worth trying the chatstate anyway — an account that is already
		// available (or a push name we could not read) should not cost us the
		// indicator — but say so, because this is the usual reason for a
		// "typing…" that never appears.
		m.logger.Warnf("typing: could not mark the account available: %v", err)
		return
	}
	m.online = true
}

// settlePresence goes back offline once nothing is being typed anywhere, so
// the account is not permanently online between replies.
func (m *typingManager) settlePresence() {
	m.mu.Lock()
	remaining := len(m.holds)
	m.mu.Unlock()
	if remaining > 0 {
		return
	}
	m.presenceMu.Lock()
	defer m.presenceMu.Unlock()
	if !m.online {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), presenceTimeout)
	defer cancel()
	if err := m.client.SendPresence(ctx, types.PresenceUnavailable); err != nil {
		m.logger.Warnf("typing: could not mark the account unavailable: %v", err)
		return
	}
	m.online = false
}

// typingKey is how a chat is addressed in the holds map: the JID without any
// device suffix, so the same chat reached two ways is one hold.
func typingKey(jid types.JID) string {
	return jid.ToNonAD().String()
}

// TypingRequest is the body of POST /api/typing.
type TypingRequest struct {
	ChatJID string `json:"chat_jid"`
	// State is "composing" (default) or "paused".
	State           string `json:"state"`
	DurationSeconds int    `json:"duration_seconds"`
}

// TypingResponse is what POST /api/typing answers with.
type TypingResponse struct {
	Success   bool      `json:"success"`
	ChatJID   string    `json:"chat_jid"`
	State     string    `json:"state"`
	Message   string    `json:"message,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

func registerTypingRoutes(mux *http.ServeMux, typing *typingManager, ready func(http.ResponseWriter) bool) {
	mux.HandleFunc("/api/typing", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			list := typing.List()
			if list == nil {
				list = []TypingStatus{}
			}
			json.NewEncoder(w).Encode(list)
			return
		case http.MethodPost:
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if ready != nil && !ready(w) {
			return
		}

		var req TypingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		if req.ChatJID == "" {
			http.Error(w, "chat_jid is required", http.StatusBadRequest)
			return
		}
		// types.ParseJID barely validates, so malformed input is caught on User rather than err alone.
		jid, err := types.ParseJID(req.ChatJID)
		if err != nil || jid.User == "" {
			http.Error(w, fmt.Sprintf("invalid chat_jid: %q", req.ChatJID), http.StatusBadRequest)
			return
		}
		state := req.State
		if state == "" {
			state = string(types.ChatPresenceComposing)
		}

		w.Header().Set("Content-Type", "application/json")
		switch state {
		case string(types.ChatPresenceComposing):
			if !typing.Enabled() {
				w.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(w).Encode(TypingResponse{
					ChatJID: typingKey(jid),
					State:   state,
					Message: "typing indicators are disabled on this bridge (WHATSAPP_BRIDGE_TYPING)",
				})
				return
			}
			status, err := typing.Start(jid, time.Duration(req.DurationSeconds)*time.Second)
			if err != nil {
				w.WriteHeader(http.StatusBadGateway)
				json.NewEncoder(w).Encode(TypingResponse{ChatJID: typingKey(jid), State: state, Message: err.Error()})
				return
			}
			json.NewEncoder(w).Encode(TypingResponse{
				Success:   true,
				ChatJID:   status.ChatJID,
				State:     state,
				ExpiresAt: status.ExpiresAt,
			})
		case string(types.ChatPresencePaused):
			if err := typing.Stop(jid); err != nil {
				w.WriteHeader(http.StatusBadGateway)
				json.NewEncoder(w).Encode(TypingResponse{ChatJID: typingKey(jid), State: state, Message: err.Error()})
				return
			}
			json.NewEncoder(w).Encode(TypingResponse{Success: true, ChatJID: typingKey(jid), State: state})
		default:
			http.Error(w, fmt.Sprintf("invalid state: %q (want composing or paused)", req.State), http.StatusBadRequest)
		}
	})
}
