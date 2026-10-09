package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	webhookKindClaudeRoutine = "claude_routine"
	webhookKindGeneric       = "generic"

	maxWebhookTextLen         = 65000
	webhookTimeout            = 15 * time.Second
	webhookMaxConsecutiveFail = 3
	webhookMaxTTLSeconds      = 30 * 24 * 3600
	webhookDefaultMaxPerHour  = 60
)

// webhookDebounceMaxWait caps how long a trailing debounce can keep putting
// a delivery off: however steadily someone keeps typing, the batch goes out
// this long after its first message (or after the debounce itself, if that
// is set longer). A package-level var so tests can shorten it; overridden by
// WHATSAPP_WEBHOOK_DEBOUNCE_MAX_WAIT_SECONDS.
var webhookDebounceMaxWait = time.Duration(envPositiveInt("WHATSAPP_WEBHOOK_DEBOUNCE_MAX_WAIT_SECONDS", 300)) * time.Second

// webhookCooldownDuration is how long a subscription is held after a failed
// delivery before the next queued batch is attempted. A package-level var
// (not a const) so tests can shorten it.
var webhookCooldownDuration = 5 * time.Minute

// WebhookSubscription is a row in webhook_subscriptions. chat_jid of "*"
// matches every chat.
type WebhookSubscription struct {
	ID                  int64
	ChatJID             string
	URL                 string
	BearerToken         string
	Kind                string
	Headers             map[string]string
	IncludeFromMe       bool
	DebounceSeconds     int
	Enabled             bool
	CreatedAt           time.Time
	LastFiredAt         *time.Time
	LastStatus          *int
	LastError           string
	ConsecutiveFailures int
	DisabledReason      string
	ExpiresAt           *time.Time
	MaxPerHour          int
}

// WebhookSubscriptionView is what the REST API returns: the bearer token is
// never echoed back, only its last 4 characters as a hint.
type WebhookSubscriptionView struct {
	ID                  int               `json:"id"`
	ChatJID             string            `json:"chat_jid"`
	URL                 string            `json:"url"`
	BearerToken         string            `json:"bearer_token"`
	BearerTokenHint     string            `json:"bearer_token_hint"`
	Kind                string            `json:"kind"`
	Headers             map[string]string `json:"headers"`
	IncludeFromMe       bool              `json:"include_from_me"`
	DebounceSeconds     int               `json:"debounce_seconds"`
	Enabled             bool              `json:"enabled"`
	CreatedAt           time.Time         `json:"created_at"`
	LastFiredAt         *time.Time        `json:"last_fired_at,omitempty"`
	LastStatus          *int              `json:"last_status,omitempty"`
	LastError           string            `json:"last_error,omitempty"`
	ConsecutiveFailures int               `json:"consecutive_failures"`
	DisabledReason      string            `json:"disabled_reason,omitempty"`
	ExpiresAt           *time.Time        `json:"expires_at,omitempty"`
	MaxPerHour          int               `json:"max_per_hour"`
}

func maskWebhookSubscription(sub WebhookSubscription) WebhookSubscriptionView {
	hint := ""
	if len(sub.BearerToken) >= 4 {
		hint = sub.BearerToken[len(sub.BearerToken)-4:]
	}
	return WebhookSubscriptionView{
		ID:                  int(sub.ID),
		ChatJID:             sub.ChatJID,
		URL:                 sub.URL,
		BearerToken:         "",
		BearerTokenHint:     hint,
		Kind:                sub.Kind,
		Headers:             sub.Headers,
		IncludeFromMe:       sub.IncludeFromMe,
		DebounceSeconds:     sub.DebounceSeconds,
		Enabled:             sub.Enabled,
		CreatedAt:           sub.CreatedAt,
		LastFiredAt:         sub.LastFiredAt,
		LastStatus:          sub.LastStatus,
		LastError:           sub.LastError,
		ConsecutiveFailures: sub.ConsecutiveFailures,
		DisabledReason:      sub.DisabledReason,
		ExpiresAt:           sub.ExpiresAt,
		MaxPerHour:          sub.MaxPerHour,
	}
}

// WebhookEvent describes one WhatsApp message as published to consumers:
// the payload of message.new / message.updated events and the body items
// of webhook deliveries.
type WebhookEvent struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
	ChatName  string `json:"chat_name"`
	Sender    string `json:"sender"`
	// SenderName is the contact name, or "Me", resolved as the read API resolves it.
	SenderName string `json:"sender_name"`
	Content    string `json:"content"`
	Timestamp  string `json:"timestamp"` // RFC3339
	IsFromMe   bool   `json:"is_from_me"`
	MediaType  string `json:"media_type,omitempty"`
	Filename   string `json:"filename,omitempty"`
	// HasMedia means the attachment is fetchable at /api/media/{chat_jid}/{message_id}.
	HasMedia bool `json:"has_media"`
	// Transcript is the spoken text of a voice note; content is left as
	// WhatsApp delivered it so consumers can tell speech from typing.
	Transcript          string `json:"transcript,omitempty"`
	TranscriptionStatus string `json:"transcription_status,omitempty"` // ok, failed, timeout, disabled
	DurationSeconds     int    `json:"duration_seconds,omitempty"`
	// Poll is set when the message creates a poll (media_type "poll").
	Poll *PollView `json:"poll,omitempty"`
	// PollVote is set on a poll.vote event: the vote and the poll's new tally.
	PollVote *PollVotePayload `json:"poll_vote,omitempty"`
}

// createWebhookSubscriptionsTable is called from NewMessageStore alongside the other tables.
func createWebhookSubscriptionsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS webhook_subscriptions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			chat_jid TEXT NOT NULL,
			url TEXT NOT NULL,
			bearer_token TEXT NOT NULL DEFAULT '',
			kind TEXT NOT NULL DEFAULT 'claude_routine',
			headers TEXT NOT NULL DEFAULT '{}',
			include_from_me INTEGER NOT NULL DEFAULT 0,
			debounce_seconds INTEGER NOT NULL DEFAULT 0,
			enabled INTEGER NOT NULL DEFAULT 1,
			created_at TIMESTAMP,
			last_fired_at TIMESTAMP,
			last_status INTEGER,
			last_error TEXT,
			consecutive_failures INTEGER NOT NULL DEFAULT 0,
			disabled_reason TEXT NOT NULL DEFAULT '',
			expires_at TIMESTAMP,
			max_per_hour INTEGER NOT NULL DEFAULT 60
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create webhook_subscriptions table: %v", err)
	}
	return nil
}

// AddWebhookSubscription inserts a new subscription and returns the stored row.
func (store *MessageStore) AddWebhookSubscription(sub WebhookSubscription) (WebhookSubscription, error) {
	headersJSON, err := json.Marshal(sub.Headers)
	if err != nil {
		return WebhookSubscription{}, fmt.Errorf("failed to encode headers: %v", err)
	}
	now := time.Now()
	if sub.MaxPerHour <= 0 {
		sub.MaxPerHour = webhookDefaultMaxPerHour
	}
	res, err := store.db.Exec(
		`INSERT INTO webhook_subscriptions
			(chat_jid, url, bearer_token, kind, headers, include_from_me, debounce_seconds, enabled, created_at, expires_at, max_per_hour)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		sub.ChatJID, sub.URL, sub.BearerToken, sub.Kind, string(headersJSON),
		sub.IncludeFromMe, sub.DebounceSeconds, sub.Enabled, now, sub.ExpiresAt, sub.MaxPerHour,
	)
	if err != nil {
		return WebhookSubscription{}, fmt.Errorf("failed to insert webhook subscription: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return WebhookSubscription{}, fmt.Errorf("failed to read inserted webhook id: %v", err)
	}
	sub.ID = id
	sub.CreatedAt = now
	return sub, nil
}

func scanWebhookSubscription(row interface {
	Scan(dest ...any) error
}) (WebhookSubscription, error) {
	var sub WebhookSubscription
	var headersJSON string
	var createdAt sql.NullTime
	var lastFiredAt sql.NullTime
	var lastStatus sql.NullInt64
	var lastError sql.NullString
	var expiresAt sql.NullTime
	if err := row.Scan(
		&sub.ID, &sub.ChatJID, &sub.URL, &sub.BearerToken, &sub.Kind, &headersJSON,
		&sub.IncludeFromMe, &sub.DebounceSeconds, &sub.Enabled,
		&createdAt, &lastFiredAt, &lastStatus, &lastError,
		&sub.ConsecutiveFailures, &sub.DisabledReason, &expiresAt, &sub.MaxPerHour,
	); err != nil {
		return WebhookSubscription{}, err
	}
	if headersJSON == "" {
		headersJSON = "{}"
	}
	if err := json.Unmarshal([]byte(headersJSON), &sub.Headers); err != nil {
		sub.Headers = map[string]string{}
	}
	if createdAt.Valid {
		sub.CreatedAt = createdAt.Time
	}
	if lastFiredAt.Valid {
		t := lastFiredAt.Time
		sub.LastFiredAt = &t
	}
	if lastStatus.Valid {
		v := int(lastStatus.Int64)
		sub.LastStatus = &v
	}
	if lastError.Valid {
		sub.LastError = lastError.String
	}
	if expiresAt.Valid {
		t := expiresAt.Time
		sub.ExpiresAt = &t
	}
	if sub.MaxPerHour <= 0 {
		sub.MaxPerHour = webhookDefaultMaxPerHour
	}
	return sub, nil
}

const webhookSelectColumns = `id, chat_jid, url, bearer_token, kind, headers, include_from_me, debounce_seconds, enabled,
	created_at, last_fired_at, last_status, last_error, consecutive_failures, disabled_reason, expires_at, max_per_hour`

// ListWebhookSubscriptions returns every subscription, newest first.
func (store *MessageStore) ListWebhookSubscriptions() ([]WebhookSubscription, error) {
	rows, err := store.db.Query(`SELECT ` + webhookSelectColumns + ` FROM webhook_subscriptions ORDER BY id DESC`)
	if err != nil {
		return nil, fmt.Errorf("failed to list webhook subscriptions: %v", err)
	}
	defer rows.Close()

	var subs []WebhookSubscription
	for rows.Next() {
		sub, err := scanWebhookSubscription(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan webhook subscription: %v", err)
		}
		subs = append(subs, sub)
	}
	return subs, rows.Err()
}

// GetWebhookSubscription looks up one subscription by id.
func (store *MessageStore) GetWebhookSubscription(id int64) (WebhookSubscription, bool, error) {
	row := store.db.QueryRow(`SELECT `+webhookSelectColumns+` FROM webhook_subscriptions WHERE id = ?`, id)
	sub, err := scanWebhookSubscription(row)
	if err == sql.ErrNoRows {
		return WebhookSubscription{}, false, nil
	}
	if err != nil {
		return WebhookSubscription{}, false, fmt.Errorf("failed to get webhook subscription: %v", err)
	}
	return sub, true, nil
}

// DeleteWebhookSubscription removes a subscription. found is false if no row matched.
func (store *MessageStore) DeleteWebhookSubscription(id int64) (found bool, err error) {
	res, err := store.db.Exec(`DELETE FROM webhook_subscriptions WHERE id = ?`, id)
	if err != nil {
		return false, fmt.Errorf("failed to delete webhook subscription: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to read delete result: %v", err)
	}
	return n > 0, nil
}

// MatchingWebhookSubscriptions returns enabled, unexpired subscriptions for a
// chat: those pinned to chatJID plus wildcard ('*') subscriptions.
func (store *MessageStore) MatchingWebhookSubscriptions(chatJID string) ([]WebhookSubscription, error) {
	rows, err := store.db.Query(
		`SELECT `+webhookSelectColumns+` FROM webhook_subscriptions
		 WHERE enabled = 1 AND (chat_jid = ? OR chat_jid = '*')
		   AND (expires_at IS NULL OR expires_at > ?)`,
		chatJID, time.Now(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to match webhook subscriptions: %v", err)
	}
	defer rows.Close()

	var subs []WebhookSubscription
	for rows.Next() {
		sub, err := scanWebhookSubscription(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan webhook subscription: %v", err)
		}
		subs = append(subs, sub)
	}
	return subs, rows.Err()
}

// recordWebhookResult updates the last-fired bookkeeping for a subscription.
func (store *MessageStore) recordWebhookResult(id int64, status int, errText string) error {
	var statusVal any
	if status > 0 {
		statusVal = status
	}
	var errVal any
	if errText != "" {
		errVal = errText
	}
	_, err := store.db.Exec(
		`UPDATE webhook_subscriptions SET last_fired_at = ?, last_status = ?, last_error = ? WHERE id = ?`,
		time.Now(), statusVal, errVal, id,
	)
	return err
}

// resetWebhookFailures clears the consecutive-failure counter after a success.
func (store *MessageStore) resetWebhookFailures(id int64) error {
	_, err := store.db.Exec(`UPDATE webhook_subscriptions SET consecutive_failures = 0 WHERE id = ?`, id)
	return err
}

// incrementWebhookFailures bumps the consecutive-failure counter and returns the new value.
func (store *MessageStore) incrementWebhookFailures(id int64) (int, error) {
	if _, err := store.db.Exec(`UPDATE webhook_subscriptions SET consecutive_failures = consecutive_failures + 1 WHERE id = ?`, id); err != nil {
		return 0, err
	}
	var n int
	if err := store.db.QueryRow(`SELECT consecutive_failures FROM webhook_subscriptions WHERE id = ?`, id).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// disableWebhookSubscription turns a subscription off and records why.
func (store *MessageStore) disableWebhookSubscription(id int64, reason string) error {
	_, err := store.db.Exec(`UPDATE webhook_subscriptions SET enabled = 0, disabled_reason = ? WHERE id = ?`, reason, id)
	return err
}

// enableWebhookSubscription turns a subscription back on and clears failure state.
func (store *MessageStore) enableWebhookSubscription(id int64) (found bool, err error) {
	res, err := store.db.Exec(
		`UPDATE webhook_subscriptions SET enabled = 1, disabled_reason = '', consecutive_failures = 0 WHERE id = ?`,
		id,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// httpDoer is the subset of *http.Client the dispatcher needs, so tests can substitute it.
type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// WebhookDispatcher fans incoming WebhookEvents out to matching subscriptions,
// coalescing bursts (from debounce_seconds, and from a post-failure cooldown)
// into a single delivery. Bursts are batched per subscription *and chat*, so
// a wildcard subscription wakes once per conversation rather than lumping
// unrelated chats together, and one chatty guest cannot hold everyone else's
// delivery back.
type WebhookDispatcher struct {
	store  *MessageStore
	client httpDoer
	logger waLog.Logger

	mu            sync.Mutex
	pending       map[batchKey]*pendingBatch
	cooldownUntil map[int64]time.Time

	rateMu sync.Mutex
	hits   map[int64][]time.Time

	// onWake, when set, is called with the chat of an incoming message once
	// it has been delivered to a subscription whose job is to wake an agent
	// that answers (kind claude_routine). It is what puts "typing…" in front
	// of the guest while that agent is spun up and thinking — see typing.go.
	// Set once at startup, before any event is dispatched.
	onWake func(chatJID string)
}

// batchKey identifies one pending batch: a subscription's events for one chat.
type batchKey struct {
	sub  int64
	chat string
}

type pendingBatch struct {
	events []WebhookEvent
	timer  *time.Timer
	// openedAt is when the batch's first event arrived; the trailing
	// debounce never pushes delivery past openedAt + webhookDebounceMaxWait.
	openedAt time.Time
}

// NewWebhookDispatcher builds a dispatcher using the real network by default.
func NewWebhookDispatcher(store *MessageStore, logger waLog.Logger) *WebhookDispatcher {
	return &WebhookDispatcher{
		store:         store,
		client:        &http.Client{Timeout: webhookTimeout},
		logger:        logger,
		pending:       make(map[batchKey]*pendingBatch),
		cooldownUntil: make(map[int64]time.Time),
		hits:          make(map[int64][]time.Time),
	}
}

// Notify is fire-and-forget: it looks up matching subscriptions and queues or
// sends the event in the background, never blocking the caller.
func (d *WebhookDispatcher) Notify(event WebhookEvent) {
	go d.dispatch(event)
}

func (d *WebhookDispatcher) dispatch(event WebhookEvent) {
	subs, err := d.store.MatchingWebhookSubscriptions(event.ChatJID)
	if err != nil {
		d.logger.Warnf("webhook: failed to look up subscriptions for %s: %v", event.ChatJID, err)
		return
	}
	for _, sub := range subs {
		if event.IsFromMe && !sub.IncludeFromMe {
			continue
		}
		d.enqueueOrSend(sub, event)
	}
}

// enqueueOrSend either fires immediately (no debounce, no active cooldown) or
// folds the event into the pending batch for its subscription and chat.
//
// The debounce is trailing: every new event restarts the window, so a batch
// goes out once the chat has been quiet for debounce_seconds, not a fixed
// time after its first message. (A fixed window split any burst that
// happened to straddle it into two deliveries — two woken agents answering
// the same conversation.) So that someone typing steadily is not kept
// waiting forever, the window never extends past webhookDebounceMaxWait
// from the batch's first event. An active cooldown still holds the batch
// until it ends, whichever is later.
func (d *WebhookDispatcher) enqueueOrSend(sub WebhookSubscription, event WebhookEvent) {
	d.mu.Lock()

	now := time.Now()
	var cooldownEnd time.Time
	if until, ok := d.cooldownUntil[sub.ID]; ok {
		if until.After(now) {
			cooldownEnd = until
		} else {
			delete(d.cooldownUntil, sub.ID)
		}
	}
	debounce := time.Duration(sub.DebounceSeconds) * time.Second
	key := batchKey{sub: sub.ID, chat: event.ChatJID}

	if batch, ok := d.pending[key]; ok {
		batch.events = append(batch.events, event)
		// If the timer has already fired, its flush is waiting on d.mu and
		// will deliver this event with the rest; the re-armed timer then
		// finds the batch gone (or replaced) and does nothing.
		batch.timer.Reset(waitFrom(batchDueAt(now, batch.openedAt, debounce, cooldownEnd)))
		d.mu.Unlock()
		return
	}

	if debounce <= 0 && cooldownEnd.IsZero() {
		d.mu.Unlock()
		go d.attemptDelivery(sub, []WebhookEvent{event}, true)
		return
	}

	batch := &pendingBatch{events: []WebhookEvent{event}, openedAt: now}
	batch.timer = time.AfterFunc(waitFrom(batchDueAt(now, now, debounce, cooldownEnd)), func() { d.flush(key, batch) })
	d.pending[key] = batch
	d.mu.Unlock()
}

// batchDueAt is when a pending batch should go out, given that an event has
// just arrived: debounce after now, but no later than the max wait from when
// the batch opened, and never before an active cooldown ends.
func batchDueAt(now, openedAt time.Time, debounce time.Duration, cooldownEnd time.Time) time.Time {
	due := now.Add(debounce)
	if limit := openedAt.Add(max(webhookDebounceMaxWait, debounce)); due.After(limit) {
		due = limit
	}
	if cooldownEnd.After(due) {
		due = cooldownEnd
	}
	return due
}

// flush delivers a pending batch: its debounce window (or cooldown) has
// elapsed, so this is the next delivery attempt. It acts only if batch is
// still the one pending under key — a timer re-armed after it had already
// fired must not deliver the next batch early.
func (d *WebhookDispatcher) flush(key batchKey, batch *pendingBatch) {
	d.mu.Lock()
	if d.pending[key] != batch {
		d.mu.Unlock()
		return
	}
	delete(d.pending, key)
	d.mu.Unlock()

	sub, found, err := d.store.GetWebhookSubscription(key.sub)
	if err != nil {
		d.logger.Warnf("webhook %d: failed to look up subscription for flush: %v", key.sub, err)
		return
	}
	if !found || !sub.Enabled {
		return
	}
	d.attemptDelivery(sub, batch.events, true)
}

// discardPending drops any batch that accumulated for a subscription while a
// delivery was in flight, and cancels its timer. Used when a subscription is
// auto-disabled: those held events will never be sent.
func (d *WebhookDispatcher) discardPending(id int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, batch := range d.pending {
		if key.sub == id {
			batch.timer.Stop()
			delete(d.pending, key)
		}
	}
	delete(d.cooldownUntil, id)
}

func (d *WebhookDispatcher) setCooldown(id int64) {
	d.mu.Lock()
	d.cooldownUntil[id] = time.Now().Add(webhookCooldownDuration)
	d.mu.Unlock()
}

func (d *WebhookDispatcher) inCooldown(id int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	until, ok := d.cooldownUntil[id]
	return ok && until.After(time.Now())
}

func (d *WebhookDispatcher) clearCooldown(id int64) {
	d.mu.Lock()
	delete(d.cooldownUntil, id)
	d.mu.Unlock()
}

// allowRate enforces the per-subscription sliding hourly cap. It is skipped
// for manual /test deliveries.
func (d *WebhookDispatcher) allowRate(sub WebhookSubscription) bool {
	limit := sub.MaxPerHour
	if limit <= 0 {
		limit = webhookDefaultMaxPerHour
	}
	now := time.Now()
	cutoff := now.Add(-time.Hour)

	d.rateMu.Lock()
	defer d.rateMu.Unlock()

	hits := d.hits[sub.ID]
	kept := hits[:0]
	for _, h := range hits {
		if h.After(cutoff) {
			kept = append(kept, h)
		}
	}
	if len(kept) >= limit {
		d.hits[sub.ID] = kept
		return false
	}
	kept = append(kept, now)
	d.hits[sub.ID] = kept
	return true
}

// attemptDelivery is the automatic delivery path: it applies the expiry and
// rate-cap checks and then makes a single delivery attempt.
func (d *WebhookDispatcher) attemptDelivery(sub WebhookSubscription, events []WebhookEvent, countRate bool) (int, error) {
	if sub.ExpiresAt != nil && !sub.ExpiresAt.After(time.Now()) {
		return 0, nil
	}
	if countRate && !d.allowRate(sub) {
		d.logger.Infof("webhook %d: rate limit reached (max_per_hour=%d), dropping delivery", sub.ID, sub.MaxPerHour)
		return 0, nil
	}
	status, err := d.deliverOnce(sub, events)
	if err == nil {
		d.wakeTyping(sub, events)
	}
	return status, err
}

// wakeTyping starts "typing…" once a claude_routine delivery has actually
// been accepted: an agent is now on its way to answer. Not earlier — during
// the debounce window the guest may still be typing themselves and nothing
// has been woken yet, and a delivery that failed wakes nothing at all. A
// generic consumer makes no promise to answer and gets no indicator, and a
// batch of only our own messages is not a reply being worked on. Manual
// /test deliveries call deliverOnce directly and so never get here.
func (d *WebhookDispatcher) wakeTyping(sub WebhookSubscription, events []WebhookEvent) {
	if d.onWake == nil || sub.Kind != webhookKindClaudeRoutine {
		return
	}
	woken := make(map[string]bool)
	for _, e := range events {
		if e.IsFromMe || woken[e.ChatJID] {
			continue
		}
		woken[e.ChatJID] = true
		d.onWake(e.ChatJID)
	}
}

// deliverOnce makes exactly one HTTP POST attempt (no in-request retries: the
// endpoint is not idempotent) and applies the auto-disable/cooldown policy to
// the outcome.
func (d *WebhookDispatcher) deliverOnce(sub WebhookSubscription, events []WebhookEvent) (int, error) {
	body, err := buildWebhookBody(sub, events)
	if err != nil {
		d.handleFailure(sub, 0, err)
		return 0, err
	}

	status, err := d.attempt(sub, body)
	if err != nil {
		d.logger.Warnf("webhook %d failed: %v", sub.ID, err)
		d.handleFailure(sub, status, err)
		return status, err
	}

	d.logger.Infof("webhook %d -> %d", sub.ID, status)
	if status < 300 {
		d.handleSuccess(sub, status)
		return status, nil
	}
	ferr := fmt.Errorf("webhook returned status %d", status)
	d.handleFailure(sub, status, ferr)
	return status, ferr
}

func (d *WebhookDispatcher) handleSuccess(sub WebhookSubscription, status int) {
	if err := d.store.recordWebhookResult(sub.ID, status, ""); err != nil {
		d.logger.Warnf("webhook %d: failed to record result: %v", sub.ID, err)
	}
	if err := d.store.resetWebhookFailures(sub.ID); err != nil {
		d.logger.Warnf("webhook %d: failed to reset failure count: %v", sub.ID, err)
	}
	d.clearCooldown(sub.ID)
}

// handleFailure records the failed attempt and either starts a cooldown
// before the next queued batch is tried, or, at webhookMaxConsecutiveFail,
// auto-disables the subscription and discards whatever is held for it.
func (d *WebhookDispatcher) handleFailure(sub WebhookSubscription, status int, err error) {
	errText := err.Error()
	if updErr := d.store.recordWebhookResult(sub.ID, status, errText); updErr != nil {
		d.logger.Warnf("webhook %d: failed to record result: %v", sub.ID, updErr)
	}

	// Concurrent deliveries that all fail during one outage count as a
	// single strike: only the first failure in a cooldown window increments.
	if d.inCooldown(sub.ID) {
		return
	}

	n, incErr := d.store.incrementWebhookFailures(sub.ID)
	if incErr != nil {
		d.logger.Warnf("webhook %d: failed to increment failure count: %v", sub.ID, incErr)
		return
	}

	if n >= webhookMaxConsecutiveFail {
		summary := errText
		if status > 0 {
			summary = fmt.Sprintf("http %d", status)
		}
		reason := fmt.Sprintf("%d consecutive failures, last: %s", n, summary)
		if updErr := d.store.disableWebhookSubscription(sub.ID, reason); updErr != nil {
			d.logger.Warnf("webhook %d: failed to auto-disable: %v", sub.ID, updErr)
			return
		}
		d.logger.Warnf("webhook %d: auto-disabled (%s)", sub.ID, reason)
		d.discardPending(sub.ID)
		return
	}

	d.setCooldown(sub.ID)
}

func (d *WebhookDispatcher) attempt(sub WebhookSubscription, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, sub.URL, bytes.NewReader(body))
	if err != nil {
		return 0, sanitizeWebhookError(err, sub)
	}
	req.Header.Set("Content-Type", "application/json")
	if sub.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+sub.BearerToken)
	}
	if sub.Kind == webhookKindClaudeRoutine {
		req.Header.Set("anthropic-version", "2023-06-01")
		req.Header.Set("anthropic-beta", "experimental-cc-routine-2026-04-01")
	}
	for k, v := range sub.Headers {
		req.Header.Set(k, v)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, sanitizeWebhookError(err, sub)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.StatusCode, nil
}

// sanitizeWebhookError strips the token and query string from an error
// message so neither ends up logged or stored.
func sanitizeWebhookError(err error, sub WebhookSubscription) error {
	msg := err.Error()
	if sub.BearerToken != "" {
		msg = strings.ReplaceAll(msg, sub.BearerToken, "***")
	}
	if u, parseErr := url.Parse(sub.URL); parseErr == nil && u.RawQuery != "" {
		msg = strings.ReplaceAll(msg, "?"+u.RawQuery, "")
	}
	return fmt.Errorf("%s", msg)
}

func buildWebhookBody(sub WebhookSubscription, events []WebhookEvent) ([]byte, error) {
	text := buildWebhookText(events)
	payload := map[string]any{
		"text":   text,
		"events": events,
	}
	return json.Marshal(payload)
}

func buildWebhookText(events []WebhookEvent) string {
	if len(events) == 0 {
		return ""
	}

	sameChat := true
	for _, e := range events[1:] {
		if e.ChatJID != events[0].ChatJID {
			sameChat = false
			break
		}
	}

	var b strings.Builder
	n := len(events)
	plural := ""
	if n != 1 {
		plural = "s"
	}
	if sameChat {
		fmt.Fprintf(&b, "%d new WhatsApp message%s in %s (%s):\n", n, plural, events[0].ChatName, events[0].ChatJID)
	} else {
		fmt.Fprintf(&b, "%d new WhatsApp message%s across multiple chats:\n", n, plural)
	}

	for _, e := range events {
		ts := e.Timestamp
		if t, err := time.Parse(time.RFC3339, e.Timestamp); err == nil {
			ts = t.Format("2006-01-02 15:04:05")
		}
		var content string
		switch {
		case e.PollVote != nil:
			content = describePollVote(e.PollVote)
		case e.Poll != nil:
			content = describePoll(e.Poll)
		case e.Transcript != "":
			dur := ""
			if e.DurationSeconds > 0 {
				dur = fmt.Sprintf(", %d:%02d", e.DurationSeconds/60, e.DurationSeconds%60)
			}
			content = fmt.Sprintf("[voice note%s] %s", dur, e.Transcript)
		case e.MediaType == "audio" && e.TranscriptionStatus != "":
			content = fmt.Sprintf("[voice note: %s, transcription %s]", e.Filename, e.TranscriptionStatus)
		case e.MediaType != "" && e.Content != "":
			content = fmt.Sprintf("[%s: %s] %s", e.MediaType, e.Filename, e.Content)
		case e.MediaType != "":
			content = fmt.Sprintf("[%s: %s]", e.MediaType, e.Filename)
		default:
			content = e.Content
		}
		who := e.Sender
		if !sameChat {
			who = fmt.Sprintf("%s (%s)", e.ChatName, e.Sender)
		}
		fmt.Fprintf(&b, "[%s] %s: %s\n", ts, who, content)
	}

	eventsJSON, err := json.Marshal(events)
	if err == nil {
		b.WriteString("\n")
		b.Write(eventsJSON)
	}

	text := b.String()
	if len(text) > maxWebhookTextLen {
		text = text[:maxWebhookTextLen]
	}
	return text
}

// --- REST API ---

// CreateWebhookRequest is the body of POST /api/webhooks.
type CreateWebhookRequest struct {
	ChatJID         string            `json:"chat_jid"`
	URL             string            `json:"url"`
	BearerToken     string            `json:"bearer_token"`
	Kind            string            `json:"kind"`
	Headers         map[string]string `json:"headers"`
	IncludeFromMe   bool              `json:"include_from_me"`
	DebounceSeconds int               `json:"debounce_seconds"`
	TTLSeconds      int               `json:"ttl_seconds"`
	MaxPerHour      int               `json:"max_per_hour"`
}

// WebhookTestResponse is the body of POST /api/webhooks/{id}/test.
type WebhookTestResponse struct {
	Success bool   `json:"success"`
	Status  int    `json:"status,omitempty"`
	Error   string `json:"error,omitempty"`
}

func registerWebhookRoutes(mux *http.ServeMux, messageStore *MessageStore, dispatcher *WebhookDispatcher, logger waLog.Logger) {
	mux.HandleFunc("/api/webhooks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			subs, err := messageStore.ListWebhookSubscriptions()
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to list webhooks: %v", err), http.StatusInternalServerError)
				return
			}
			views := make([]WebhookSubscriptionView, 0, len(subs))
			for _, sub := range subs {
				views = append(views, maskWebhookSubscription(sub))
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(views)

		case http.MethodPost:
			var req CreateWebhookRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "Invalid request format", http.StatusBadRequest)
				return
			}
			if req.ChatJID == "" {
				http.Error(w, "chat_jid is required", http.StatusBadRequest)
				return
			}
			if req.URL == "" {
				http.Error(w, "url is required", http.StatusBadRequest)
				return
			}
			parsed, err := url.Parse(req.URL)
			if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
				http.Error(w, "url must be a valid http(s) URL", http.StatusBadRequest)
				return
			}
			if req.Kind == "" {
				req.Kind = webhookKindClaudeRoutine
			}
			if req.Kind != webhookKindClaudeRoutine && req.Kind != webhookKindGeneric {
				http.Error(w, "kind must be claude_routine or generic", http.StatusBadRequest)
				return
			}
			if req.DebounceSeconds < 0 || req.DebounceSeconds > 3600 {
				http.Error(w, "debounce_seconds must be between 0 and 3600", http.StatusBadRequest)
				return
			}
			if req.TTLSeconds < 0 || req.TTLSeconds > webhookMaxTTLSeconds {
				http.Error(w, "ttl_seconds must be between 0 and 2592000 (30 days)", http.StatusBadRequest)
				return
			}
			if req.MaxPerHour == 0 {
				req.MaxPerHour = webhookDefaultMaxPerHour
			}
			if req.MaxPerHour < 1 || req.MaxPerHour > 3600 {
				http.Error(w, "max_per_hour must be between 1 and 3600", http.StatusBadRequest)
				return
			}
			if req.Headers == nil {
				req.Headers = map[string]string{}
			}

			var expiresAt *time.Time
			if req.TTLSeconds > 0 {
				t := time.Now().Add(time.Duration(req.TTLSeconds) * time.Second)
				expiresAt = &t
			}

			sub, err := messageStore.AddWebhookSubscription(WebhookSubscription{
				ChatJID:         req.ChatJID,
				URL:             req.URL,
				BearerToken:     req.BearerToken,
				Kind:            req.Kind,
				Headers:         req.Headers,
				IncludeFromMe:   req.IncludeFromMe,
				DebounceSeconds: req.DebounceSeconds,
				Enabled:         true,
				ExpiresAt:       expiresAt,
				MaxPerHour:      req.MaxPerHour,
			})
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to create webhook: %v", err), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(maskWebhookSubscription(sub))

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/webhooks/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "invalid webhook id", http.StatusBadRequest)
			return
		}
		found, err := messageStore.DeleteWebhookSubscription(id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete webhook: %v", err), http.StatusInternalServerError)
			return
		}
		if !found {
			http.Error(w, "webhook not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("/api/webhooks/{id}/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "invalid webhook id", http.StatusBadRequest)
			return
		}
		sub, found, err := messageStore.GetWebhookSubscription(id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to look up webhook: %v", err), http.StatusInternalServerError)
			return
		}
		if !found {
			http.Error(w, "webhook not found", http.StatusNotFound)
			return
		}

		event := WebhookEvent{
			MessageID:  "test",
			ChatJID:    sub.ChatJID,
			ChatName:   sub.ChatJID,
			Sender:     "whatsapp-bridge",
			SenderName: "whatsapp-bridge",
			Content:    "Test event from whatsapp-bridge",
			Timestamp:  time.Now().Format(time.RFC3339),
			IsFromMe:   false,
		}
		// A manual test is a single synchronous attempt: no rate cap, no cooldown/queueing.
		status, sendErr := dispatcher.deliverOnce(sub, []WebhookEvent{event})

		resp := WebhookTestResponse{Success: sendErr == nil, Status: status}
		if sendErr != nil {
			resp.Error = sendErr.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/api/webhooks/{id}/enable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "invalid webhook id", http.StatusBadRequest)
			return
		}
		found, err := messageStore.enableWebhookSubscription(id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to enable webhook: %v", err), http.StatusInternalServerError)
			return
		}
		if !found {
			http.Error(w, "webhook not found", http.StatusNotFound)
			return
		}
		dispatcher.discardPending(id)
		sub, _, err := messageStore.GetWebhookSubscription(id)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to look up webhook: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(maskWebhookSubscription(sub))
	})
}
