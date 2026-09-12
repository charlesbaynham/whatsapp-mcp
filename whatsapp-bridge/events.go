package main

// The event log: an append-only table written at the moment something
// becomes publishable, and the single source both the SSE stream and the
// webhook dispatcher are fed from. The row id is the stream cursor.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	eventMessageNew     = "message.new"
	eventMessageUpdated = "message.updated"
	eventChatRead       = "chat.read"
	eventBridgeStatus   = "bridge.status"

	sseReplayBatch   = 500
	sseHeartbeat     = 15 * time.Second
	sseSubscriberBuf = 64
)

// Event is one row of the event log as clients see it.
type Event struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	ChatJID   string          `json:"chat_jid,omitempty"`
	MessageID string          `json:"message_id,omitempty"`
	IsFromMe  bool            `json:"is_from_me,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// ChatReadPayload is the data of a chat.read event.
type ChatReadPayload struct {
	ChatJID string    `json:"chat_jid"`
	UpTo    time.Time `json:"up_to"`
	Source  string    `json:"source"` // "api", "receipt", "app_state"
}

// BridgeStatusPayload is the data of a bridge.status event.
type BridgeStatusPayload struct {
	Connected bool   `json:"connected"`
	LoggedIn  bool   `json:"logged_in"`
	JID       string `json:"jid,omitempty"`
}

func createEventsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type TEXT NOT NULL,
			chat_jid TEXT NOT NULL DEFAULT '',
			message_id TEXT NOT NULL DEFAULT '',
			is_from_me INTEGER NOT NULL DEFAULT 0,
			payload TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL
		);
		CREATE INDEX IF NOT EXISTS events_chat_idx ON events(chat_jid, id);
	`)
	if err != nil {
		return fmt.Errorf("failed to create events table: %v", err)
	}
	return nil
}

// AppendEvent writes one event and returns it with its id assigned.
func (store *MessageStore) AppendEvent(ev Event) (Event, error) {
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = time.Now()
	}
	res, err := store.db.Exec(
		`INSERT INTO events (type, chat_jid, message_id, is_from_me, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		ev.Type, ev.ChatJID, ev.MessageID, ev.IsFromMe, string(ev.Data), ev.CreatedAt,
	)
	if err != nil {
		return Event{}, fmt.Errorf("failed to append event: %v", err)
	}
	ev.ID, err = res.LastInsertId()
	if err != nil {
		return Event{}, err
	}
	return ev, nil
}

// ListEvents returns up to limit events with id > since, oldest first.
func (store *MessageStore) ListEvents(since int64, limit int) ([]Event, error) {
	rows, err := store.db.Query(
		`SELECT id, type, chat_jid, message_id, is_from_me, payload, created_at FROM events WHERE id > ? ORDER BY id ASC LIMIT ?`,
		since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		var payload string
		if err := rows.Scan(&ev.ID, &ev.Type, &ev.ChatJID, &ev.MessageID, &ev.IsFromMe, &payload, &ev.CreatedAt); err != nil {
			return nil, err
		}
		ev.Data = json.RawMessage(payload)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// LatestEventID returns the newest event id, or 0 if the log is empty.
func (store *MessageStore) LatestEventID() (int64, error) {
	var id sql.NullInt64
	if err := store.db.QueryRow(`SELECT MAX(id) FROM events`).Scan(&id); err != nil {
		return 0, err
	}
	return id.Int64, nil
}

// sseHub fans live events out to connected stream handlers. A subscriber
// that falls behind (its buffer fills) is closed rather than blocked or
// silently skipped: the handler ends the response, the client reconnects
// with Last-Event-ID, and the replay from the log fills the gap.
type sseHub struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

func newSSEHub() *sseHub {
	return &sseHub{subs: map[chan Event]struct{}{}}
}

func (h *sseHub) subscribe() (<-chan Event, func()) {
	ch := make(chan Event, sseSubscriberBuf)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
		h.mu.Unlock()
	}
}

func (h *sseHub) broadcast(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- ev:
		default:
			delete(h.subs, ch)
			close(ch)
		}
	}
}

func (h *sseHub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// Publisher is the one place events enter the system: it appends to the
// log, then fans out to the SSE hub and (for new messages) the webhook
// dispatcher.
type Publisher struct {
	store      *MessageStore
	hub        *sseHub
	dispatcher *WebhookDispatcher
	logger     waLog.Logger
}

func NewPublisher(store *MessageStore, dispatcher *WebhookDispatcher, logger waLog.Logger) *Publisher {
	return &Publisher{store: store, hub: newSSEHub(), dispatcher: dispatcher, logger: logger}
}

// Publish appends and fans out a generic event.
func (p *Publisher) Publish(typ, chatJID, messageID string, isFromMe bool, payload any) (Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	ev, err := p.store.AppendEvent(Event{Type: typ, ChatJID: chatJID, MessageID: messageID, IsFromMe: isFromMe, Data: data})
	if err != nil {
		return Event{}, err
	}
	p.hub.broadcast(ev)
	return ev, nil
}

// PublishMessage publishes a new message to every consumer.
func (p *Publisher) PublishMessage(msg WebhookEvent) {
	if _, err := p.Publish(eventMessageNew, msg.ChatJID, msg.MessageID, msg.IsFromMe, msg); err != nil {
		p.logger.Warnf("Failed to publish message event: %v", err)
	}
	if p.dispatcher != nil {
		p.dispatcher.Notify(msg)
	}
}

// PublishMessageUpdated publishes a change to an existing message (an
// on-demand transcript, an edit). Webhooks are not fired for updates.
func (p *Publisher) PublishMessageUpdated(msg WebhookEvent) {
	if _, err := p.Publish(eventMessageUpdated, msg.ChatJID, msg.MessageID, msg.IsFromMe, msg); err != nil {
		p.logger.Warnf("Failed to publish message.updated event: %v", err)
	}
}

func (p *Publisher) PublishChatRead(chatJID string, upTo time.Time, source string) {
	if _, err := p.Publish(eventChatRead, chatJID, "", false, ChatReadPayload{ChatJID: chatJID, UpTo: upTo, Source: source}); err != nil {
		p.logger.Warnf("Failed to publish chat.read event: %v", err)
	}
}

func (p *Publisher) PublishBridgeStatus(connected, loggedIn bool, jid string) {
	if _, err := p.Publish(eventBridgeStatus, "", "", false, BridgeStatusPayload{Connected: connected, LoggedIn: loggedIn, JID: jid}); err != nil {
		p.logger.Warnf("Failed to publish bridge.status event: %v", err)
	}
}

// eventFilter is the query-string filter of one GET /api/events stream.
type eventFilter struct {
	chatJID       string
	types         map[string]bool
	includeFromMe bool
}

func (f eventFilter) allows(ev Event) bool {
	if f.chatJID != "" && ev.ChatJID != "" && ev.ChatJID != f.chatJID {
		return false
	}
	if len(f.types) > 0 && !f.types[ev.Type] {
		return false
	}
	if !f.includeFromMe && ev.IsFromMe {
		return false
	}
	return true
}

func parseEventFilter(r *http.Request) (eventFilter, error) {
	f := eventFilter{chatJID: r.URL.Query().Get("chat_jid")}
	if ts := r.URL.Query().Get("types"); ts != "" {
		f.types = map[string]bool{}
		for _, t := range strings.Split(ts, ",") {
			if t = strings.TrimSpace(t); t != "" {
				f.types[t] = true
			}
		}
	}
	var err error
	f.includeFromMe, err = queryBool(r, "include_from_me", false)
	return f, err
}

// parseSince reads the cursor from ?since= or the Last-Event-ID header.
func parseSince(r *http.Request) (int64, error) {
	s := r.URL.Query().Get("since")
	if s == "" {
		s = r.Header.Get("Last-Event-ID")
	}
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("since must be a non-negative integer")
	}
	return n, nil
}

func writeSSE(w http.ResponseWriter, flusher http.Flusher, ev Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.ID, ev.Type, body); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// registerEventRoutes serves GET /api/events as text/event-stream: replay
// from the log after the cursor, then live. Subscribe-before-replay plus
// de-duplication by id guarantees no gap between the two.
func registerEventRoutes(mux *http.ServeMux, store *MessageStore, pub *Publisher) {
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "streaming unsupported")
			return
		}
		since, err := parseSince(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		filter, err := parseEventFilter(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, ": whatsapp-bridge events since %d\n\n", since)
		flusher.Flush()

		live, unsubscribe := pub.hub.subscribe()
		defer unsubscribe()

		last := since
		for {
			batch, err := store.ListEvents(last, sseReplayBatch)
			if err != nil {
				return
			}
			for _, ev := range batch {
				last = ev.ID
				if !filter.allows(ev) {
					continue
				}
				if err := writeSSE(w, flusher, ev); err != nil {
					return
				}
			}
			if len(batch) < sseReplayBatch {
				break
			}
		}

		ctx := r.Context()
		heartbeat := time.NewTicker(sseHeartbeat)
		defer heartbeat.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeat.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
					return
				}
				flusher.Flush()
			case ev, ok := <-live:
				if !ok {
					// Fell behind: end the stream so the client reconnects and replays.
					return
				}
				if ev.ID <= last {
					continue
				}
				last = ev.ID
				if !filter.allows(ev) {
					continue
				}
				if err := writeSSE(w, flusher, ev); err != nil {
					return
				}
			}
		}
	})
}

// waitForEvent is a test helper: blocks until an event with id > since is
// appended or ctx ends.
func (store *MessageStore) waitForEvent(ctx context.Context, since int64) (Event, bool) {
	for {
		evs, err := store.ListEvents(since, 1)
		if err == nil && len(evs) > 0 {
			return evs[0], true
		}
		select {
		case <-ctx.Done():
			return Event{}, false
		case <-time.After(10 * time.Millisecond):
		}
	}
}
