package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type chatPresenceCall struct {
	jid   types.JID
	state types.ChatPresence
}

// fakePresence stands in for the whatsmeow client: it records what would have
// gone to WhatsApp and can be made to fail.
type fakePresence struct {
	mu        sync.Mutex
	chatCalls []chatPresenceCall
	presences []types.Presence
	chatErr   error
}

func (f *fakePresence) SendPresence(_ context.Context, state types.Presence) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.presences = append(f.presences, state)
	return nil
}

func (f *fakePresence) SendChatPresence(_ context.Context, jid types.JID, state types.ChatPresence, _ types.ChatPresenceMedia) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.chatErr != nil {
		return f.chatErr
	}
	f.chatCalls = append(f.chatCalls, chatPresenceCall{jid: jid, state: state})
	return nil
}

func (f *fakePresence) count(state types.ChatPresence) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.chatCalls {
		if c.state == state {
			n++
		}
	}
	return n
}

func (f *fakePresence) last() (chatPresenceCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.chatCalls) == 0 {
		return chatPresenceCall{}, false
	}
	return f.chatCalls[len(f.chatCalls)-1], true
}

func (f *fakePresence) presenceStates() []types.Presence {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]types.Presence(nil), f.presences...)
}

// newFastTypingManager refreshes every few milliseconds so a test can watch
// several cycles without sleeping for seconds.
func newFastTypingManager(t *testing.T) (*typingManager, *fakePresence) {
	t.Helper()
	fake := &fakePresence{}
	m := newTypingManager(fake, waLog.Noop)
	m.refresh = 5 * time.Millisecond
	m.ttl = time.Minute
	t.Cleanup(func() {
		for _, hold := range m.List() {
			jid, _ := types.ParseJID(hold.ChatJID)
			m.Stop(jid)
		}
	})
	return m, fake
}

func typingTestJID(t *testing.T) types.JID {
	t.Helper()
	jid, err := types.ParseJID("447700900123@s.whatsapp.net")
	if err != nil {
		t.Fatalf("ParseJID: %v", err)
	}
	return jid
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestTypingStartShowsComposingAndKeepsRefreshingIt(t *testing.T) {
	m, fake := newFastTypingManager(t)
	jid := typingTestJID(t)

	status, err := m.Start(jid, 0)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if status.ChatJID != jid.String() {
		t.Fatalf("status.ChatJID = %q, want %q", status.ChatJID, jid.String())
	}
	if fake.count(types.ChatPresenceComposing) < 1 {
		t.Fatalf("expected composing to be sent immediately, got none")
	}
	// The indicator times out on WhatsApp's side after a few seconds, so the
	// hold has to keep repeating it.
	waitUntil(t, "the composing chatstate to be refreshed", func() bool {
		return fake.count(types.ChatPresenceComposing) >= 3
	})
}

func TestTypingStopSendsPausedAndEndsTheRefresh(t *testing.T) {
	m, fake := newFastTypingManager(t)
	jid := typingTestJID(t)

	if _, err := m.Start(jid, 0); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitUntil(t, "a refresh", func() bool { return fake.count(types.ChatPresenceComposing) >= 2 })
	if err := m.Stop(jid); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if fake.count(types.ChatPresencePaused) != 1 {
		t.Fatalf("expected one paused chatstate, got %d", fake.count(types.ChatPresencePaused))
	}
	if m.Active(jid.String()) {
		t.Fatalf("hold still active after Stop")
	}
	// Nothing further may go out: a composing after the paused would put the
	// indicator back up for a reply that is not coming.
	settled := fake.count(types.ChatPresenceComposing)
	time.Sleep(40 * time.Millisecond)
	if got := fake.count(types.ChatPresenceComposing); got != settled {
		t.Fatalf("refresher kept running after Stop: %d composing, was %d", got, settled)
	}
	if last, _ := fake.last(); last.state != types.ChatPresencePaused {
		t.Fatalf("last chatstate = %q, want paused", last.state)
	}
}

func TestTypingHoldLapsesOnItsOwnDeadline(t *testing.T) {
	m, fake := newFastTypingManager(t)
	jid := typingTestJID(t)

	// The session that started this one dies without ever rescinding it.
	if _, err := m.Start(jid, 20*time.Millisecond); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitUntil(t, "the hold to lapse", func() bool { return !m.Active(jid.String()) })
	if fake.count(types.ChatPresencePaused) != 1 {
		t.Fatalf("a lapsed hold must still clear the indicator, got %d paused", fake.count(types.ChatPresencePaused))
	}
}

func TestTypingStartExtendsAnExistingHoldRatherThanStacking(t *testing.T) {
	m, _ := newFastTypingManager(t)
	jid := typingTestJID(t)

	first, err := m.Start(jid, 30*time.Second)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	second, err := m.Start(jid, 5*time.Minute)
	if err != nil {
		t.Fatalf("Start (extend): %v", err)
	}
	if !second.ExpiresAt.After(first.ExpiresAt) {
		t.Fatalf("second Start did not extend the deadline: %v then %v", first.ExpiresAt, second.ExpiresAt)
	}
	if !second.StartedAt.Equal(first.StartedAt) {
		t.Fatalf("extending restarted the hold: %v then %v", first.StartedAt, second.StartedAt)
	}
	if holds := m.List(); len(holds) != 1 {
		t.Fatalf("got %d holds for one chat, want 1", len(holds))
	}

	// A shorter ttl must not cut an existing hold short.
	third, err := m.Start(jid, time.Second)
	if err != nil {
		t.Fatalf("Start (shorter): %v", err)
	}
	if !third.ExpiresAt.Equal(second.ExpiresAt) {
		t.Fatalf("a shorter ttl shortened the hold: %v, want %v", third.ExpiresAt, second.ExpiresAt)
	}
}

func TestTypingTTLIsClampedToTheMaximum(t *testing.T) {
	m, _ := newFastTypingManager(t)
	jid := typingTestJID(t)

	status, err := m.Start(jid, 10*time.Hour)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := time.Until(status.ExpiresAt); got > maxTypingTTL+time.Minute {
		t.Fatalf("hold lasts %s, want at most %s", got, maxTypingTTL)
	}
}

func TestTypingMarksTheAccountAvailableThenOfflineAgain(t *testing.T) {
	m, fake := newFastTypingManager(t)
	jid := typingTestJID(t)

	if _, err := m.Start(jid, 0); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// WhatsApp only shows chat presence from a client it believes is online.
	if states := fake.presenceStates(); len(states) != 1 || states[0] != types.PresenceAvailable {
		t.Fatalf("presence = %v, want one available", states)
	}
	waitUntil(t, "several refreshes", func() bool { return fake.count(types.ChatPresenceComposing) >= 3 })
	if states := fake.presenceStates(); len(states) != 1 {
		t.Fatalf("presence resent on every refresh: %v", states)
	}

	if err := m.Stop(jid); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	states := fake.presenceStates()
	if len(states) != 2 || states[1] != types.PresenceUnavailable {
		t.Fatalf("presence = %v, want available then unavailable once idle", states)
	}
}

func TestTypingStaysOnlineWhileAnotherChatIsStillTyping(t *testing.T) {
	m, fake := newFastTypingManager(t)
	one := typingTestJID(t)
	two, _ := types.ParseJID("447700900999@s.whatsapp.net")

	m.Start(one, 0)
	m.Start(two, 0)
	if err := m.Stop(one); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if states := fake.presenceStates(); len(states) != 1 {
		t.Fatalf("presence = %v, want to stay available while %s is still typing", states, two)
	}
}

func TestStopChatOnlyPausesAChatThatWasTyping(t *testing.T) {
	m, fake := newFastTypingManager(t)
	jid := typingTestJID(t)

	// An ordinary outgoing message to a chat nobody was typing for.
	m.StopChat("447700900555@s.whatsapp.net")
	if fake.count(types.ChatPresencePaused) != 0 {
		t.Fatalf("paused sent for a chat with no hold")
	}

	m.Start(jid, 0)
	m.StopChat(jid.String())
	if fake.count(types.ChatPresencePaused) != 1 {
		t.Fatalf("expected the send to clear the indicator, got %d paused", fake.count(types.ChatPresencePaused))
	}
	if m.Active(jid.String()) {
		t.Fatalf("hold survived the send")
	}
}

func TestTypingDisabledSendsNothing(t *testing.T) {
	m, fake := newFastTypingManager(t)
	m.enabled = false
	jid := typingTestJID(t)

	if _, err := m.Start(jid, 0); err != nil {
		t.Fatalf("Start: %v", err)
	}
	m.WakeForChat(jid.String())
	if err := m.Stop(jid); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(fake.chatCalls) != 0 || len(fake.presenceStates()) != 0 {
		t.Fatalf("disabled manager talked to WhatsApp: %v %v", fake.chatCalls, fake.presenceStates())
	}
}

func TestTypingStartLeavesNoHoldWhenWhatsAppRefusesIt(t *testing.T) {
	m, fake := newFastTypingManager(t)
	fake.chatErr = errors.New("not connected")
	jid := typingTestJID(t)

	if _, err := m.Start(jid, 0); err == nil {
		t.Fatalf("Start: expected an error")
	}
	if m.Active(jid.String()) {
		t.Fatalf("a hold was left behind for an indicator that never showed")
	}
}

// --- REST routes ---

func typingTestServer(t *testing.T, m *typingManager) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	registerTypingRoutes(mux, m, nil)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func postTyping(t *testing.T, srv *httptest.Server, body map[string]any) (*http.Response, TypingResponse) {
	t.Helper()
	raw, _ := json.Marshal(body)
	resp, err := http.Post(srv.URL+"/api/typing", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST /api/typing: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	var out TypingResponse
	json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func TestTypingRouteStartsAndStopsAHold(t *testing.T) {
	m, fake := newFastTypingManager(t)
	srv := typingTestServer(t, m)

	resp, out := postTyping(t, srv, map[string]any{"chat_jid": "447700900123@s.whatsapp.net", "duration_seconds": 90})
	if resp.StatusCode != http.StatusOK || !out.Success {
		t.Fatalf("start: status=%d body=%+v", resp.StatusCode, out)
	}
	if out.ExpiresAt.IsZero() {
		t.Fatalf("start did not report a deadline: %+v", out)
	}
	if !m.Active("447700900123@s.whatsapp.net") {
		t.Fatalf("no hold after a composing request")
	}

	listResp, err := http.Get(srv.URL + "/api/typing")
	if err != nil {
		t.Fatalf("GET /api/typing: %v", err)
	}
	defer listResp.Body.Close()
	var holds []TypingStatus
	json.NewDecoder(listResp.Body).Decode(&holds)
	if len(holds) != 1 || holds[0].ChatJID != "447700900123@s.whatsapp.net" {
		t.Fatalf("GET /api/typing = %+v, want the one hold", holds)
	}

	resp, out = postTyping(t, srv, map[string]any{"chat_jid": "447700900123@s.whatsapp.net", "state": "paused"})
	if resp.StatusCode != http.StatusOK || !out.Success {
		t.Fatalf("stop: status=%d body=%+v", resp.StatusCode, out)
	}
	if m.Active("447700900123@s.whatsapp.net") {
		t.Fatalf("hold survived a paused request")
	}
	if fake.count(types.ChatPresencePaused) != 1 {
		t.Fatalf("expected one paused chatstate, got %d", fake.count(types.ChatPresencePaused))
	}
}

func TestTypingRouteRejectsBadRequests(t *testing.T) {
	m, _ := newFastTypingManager(t)
	srv := typingTestServer(t, m)

	if resp, _ := postTyping(t, srv, map[string]any{"state": "composing"}); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing chat_jid: status=%d, want 400", resp.StatusCode)
	}
	if resp, _ := postTyping(t, srv, map[string]any{"chat_jid": "@s.whatsapp.net"}); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("malformed chat_jid: status=%d, want 400", resp.StatusCode)
	}
	if resp, _ := postTyping(t, srv, map[string]any{"chat_jid": "447700900123@s.whatsapp.net", "state": "recording"}); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown state: status=%d, want 400", resp.StatusCode)
	}
}

func TestTypingRouteSaysSoWhenTheFeatureIsOff(t *testing.T) {
	m, _ := newFastTypingManager(t)
	m.enabled = false
	srv := typingTestServer(t, m)

	resp, out := postTyping(t, srv, map[string]any{"chat_jid": "447700900123@s.whatsapp.net"})
	if resp.StatusCode != http.StatusServiceUnavailable || out.Success {
		t.Fatalf("status=%d body=%+v, want a 503 naming the switch", resp.StatusCode, out)
	}
}

// --- The webhook hook ---

// wakeRecorder is an onWake that remembers which chats it was called for.
type wakeRecorder struct {
	mu    sync.Mutex
	chats []string
}

func (w *wakeRecorder) wake(chatJID string) {
	w.mu.Lock()
	w.chats = append(w.chats, chatJID)
	w.mu.Unlock()
}

func (w *wakeRecorder) got() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.chats...)
}

func addTestSub(t *testing.T, store *MessageStore, sub WebhookSubscription) WebhookSubscription {
	t.Helper()
	sub.Headers = map[string]string{}
	sub.Enabled = true
	got, err := store.AddWebhookSubscription(sub)
	if err != nil {
		t.Fatalf("AddWebhookSubscription: %v", err)
	}
	return got
}

func TestDeliveryStartsTypingForAnAgentSubscription(t *testing.T) {
	store := newTestStore(t)
	srv, _ := newRecordingServer(t, 200)
	sub := addTestSub(t, store, WebhookSubscription{ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindClaudeRoutine})

	d := testDispatcher(store)
	rec := &wakeRecorder{}
	d.onWake = rec.wake

	// Two messages from one chat in a batch wake it once.
	if _, err := d.attemptDelivery(sub, []WebhookEvent{testEvent("111@s.whatsapp.net"), testEvent("111@s.whatsapp.net")}, true); err != nil {
		t.Fatalf("attemptDelivery: %v", err)
	}
	if got := rec.got(); len(got) != 1 || got[0] != "111@s.whatsapp.net" {
		t.Fatalf("onWake called with %v, want the one chat once", got)
	}
}

func TestTypingWaitsForTheDebouncedDelivery(t *testing.T) {
	store := newTestStore(t)
	srv, recSrv := newRecordingServer(t, 200)
	addTestSub(t, store, WebhookSubscription{ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindClaudeRoutine, DebounceSeconds: 60})

	d := testDispatcher(store)
	rec := &wakeRecorder{}
	d.onWake = rec.wake

	d.dispatch(testEvent("111@s.whatsapp.net"))
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("typing started before delivery: %v", got)
	}

	d.mu.Lock()
	var key batchKey
	var batch *pendingBatch
	for k, b := range d.pending {
		key, batch = k, b
	}
	d.mu.Unlock()
	if batch == nil {
		t.Fatal("expected a pending batch")
	}
	batch.timer.Stop()
	d.flush(key, batch)

	if recSrv.count() != 1 {
		t.Fatalf("deliveries=%d, want 1", recSrv.count())
	}
	if got := rec.got(); len(got) != 1 || got[0] != "111@s.whatsapp.net" {
		t.Fatalf("onWake called with %v after flush, want the one chat", got)
	}
}

func TestFailedDeliveryDoesNotStartTyping(t *testing.T) {
	store := newTestStore(t)
	srv, _ := newRecordingServer(t, 500)
	sub := addTestSub(t, store, WebhookSubscription{ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindClaudeRoutine})

	d := testDispatcher(store)
	rec := &wakeRecorder{}
	d.onWake = rec.wake

	d.attemptDelivery(sub, []WebhookEvent{testEvent("111@s.whatsapp.net")}, true)
	if got := rec.got(); len(got) != 0 {
		t.Fatalf("onWake called with %v after a failed delivery, want none", got)
	}
}

func TestDeliveryDoesNotStartTypingForGenericOrOwnMessages(t *testing.T) {
	store := newTestStore(t)
	srv, _ := newRecordingServer(t, 200)
	// A generic consumer is a listener, not something that answers.
	generic := addTestSub(t, store, WebhookSubscription{ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric})
	agent := addTestSub(t, store, WebhookSubscription{ChatJID: "222@s.whatsapp.net", URL: srv.URL, Kind: webhookKindClaudeRoutine, IncludeFromMe: true})

	d := testDispatcher(store)
	rec := &wakeRecorder{}
	d.onWake = rec.wake

	d.attemptDelivery(generic, []WebhookEvent{testEvent("111@s.whatsapp.net")}, true)
	ownMessage := testEvent("222@s.whatsapp.net")
	ownMessage.IsFromMe = true
	d.attemptDelivery(agent, []WebhookEvent{ownMessage}, true)

	if got := rec.got(); len(got) != 0 {
		t.Fatalf("onWake called with %v, want none", got)
	}
}

func TestWakeForChatIgnoresRubbishJIDs(t *testing.T) {
	m, fake := newFastTypingManager(t)
	m.WakeForChat("")
	m.WakeForChat("@s.whatsapp.net")
	if len(fake.chatCalls) != 0 {
		t.Fatalf("expected nothing sent, got %v", fake.chatCalls)
	}
}
