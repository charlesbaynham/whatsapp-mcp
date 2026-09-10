package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func newTestStore(t *testing.T) *MessageStore {
	t.Helper()
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMessageStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func testDispatcher(store *MessageStore) *WebhookDispatcher {
	return NewWebhookDispatcher(store, waLog.Noop)
}

func testEvent(chatJID string) WebhookEvent {
	return WebhookEvent{
		MessageID: "msg1",
		ChatJID:   chatJID,
		ChatName:  "Test Chat",
		Sender:    "12345",
		Content:   "hello",
		Timestamp: time.Now().Format(time.RFC3339),
	}
}

// --- Store CRUD ---

func TestWebhookSubscriptionCRUD(t *testing.T) {
	store := newTestStore(t)

	sub, err := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "123@s.whatsapp.net",
		URL:     "https://example.com/hook",
		Kind:    webhookKindGeneric,
		Headers: map[string]string{},
		Enabled: true,
	})
	if err != nil {
		t.Fatalf("AddWebhookSubscription: %v", err)
	}
	if sub.ID == 0 {
		t.Fatalf("expected non-zero id")
	}
	if sub.MaxPerHour != webhookDefaultMaxPerHour {
		t.Fatalf("MaxPerHour = %d, want default %d", sub.MaxPerHour, webhookDefaultMaxPerHour)
	}

	got, found, err := store.GetWebhookSubscription(sub.ID)
	if err != nil || !found {
		t.Fatalf("GetWebhookSubscription: found=%v err=%v", found, err)
	}
	if got.URL != sub.URL {
		t.Fatalf("URL = %q, want %q", got.URL, sub.URL)
	}

	list, err := store.ListWebhookSubscriptions()
	if err != nil || len(list) != 1 {
		t.Fatalf("ListWebhookSubscriptions: %d entries, err=%v", len(list), err)
	}

	deleted, err := store.DeleteWebhookSubscription(sub.ID)
	if err != nil || !deleted {
		t.Fatalf("DeleteWebhookSubscription: deleted=%v err=%v", deleted, err)
	}
	_, found, err = store.GetWebhookSubscription(sub.ID)
	if err != nil || found {
		t.Fatalf("expected subscription gone, found=%v err=%v", found, err)
	}
}

func TestMatchingWebhookSubscriptionsIncludesWildcard(t *testing.T) {
	store := newTestStore(t)

	specific, _ := store.AddWebhookSubscription(WebhookSubscription{ChatJID: "111@s.whatsapp.net", URL: "https://example.com/a", Headers: map[string]string{}, Enabled: true})
	wildcard, _ := store.AddWebhookSubscription(WebhookSubscription{ChatJID: "*", URL: "https://example.com/b", Headers: map[string]string{}, Enabled: true})
	_, _ = store.AddWebhookSubscription(WebhookSubscription{ChatJID: "222@s.whatsapp.net", URL: "https://example.com/c", Headers: map[string]string{}, Enabled: true})

	matches, err := store.MatchingWebhookSubscriptions("111@s.whatsapp.net")
	if err != nil {
		t.Fatalf("MatchingWebhookSubscriptions: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("got %d matches, want 2", len(matches))
	}
	ids := map[int64]bool{}
	for _, m := range matches {
		ids[m.ID] = true
	}
	if !ids[specific.ID] || !ids[wildcard.ID] {
		t.Fatalf("expected specific and wildcard subscriptions matched, got %+v", matches)
	}
}

func TestMatchingWebhookSubscriptionsExcludesExpired(t *testing.T) {
	store := newTestStore(t)
	past := time.Now().Add(-time.Minute)
	sub, err := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: "https://example.com/a", Headers: map[string]string{}, Enabled: true, ExpiresAt: &past,
	})
	if err != nil {
		t.Fatalf("AddWebhookSubscription: %v", err)
	}

	matches, err := store.MatchingWebhookSubscriptions("111@s.whatsapp.net")
	if err != nil {
		t.Fatalf("MatchingWebhookSubscriptions: %v", err)
	}
	for _, m := range matches {
		if m.ID == sub.ID {
			t.Fatalf("expired subscription %d should not match", sub.ID)
		}
	}
}

// --- Dispatch behavior ---

// recordingServer counts and captures every request it receives.
type recordingServer struct {
	mu       sync.Mutex
	requests []capturedRequest
}

type capturedRequest struct {
	Authorization string
	AnthropicVer  string
	AnthropicBeta string
	Body          map[string]any
}

func newRecordingServer(t *testing.T, status int) (*httptest.Server, *recordingServer) {
	rec := &recordingServer{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		rec.mu.Lock()
		rec.requests = append(rec.requests, capturedRequest{
			Authorization: r.Header.Get("Authorization"),
			AnthropicVer:  r.Header.Get("anthropic-version"),
			AnthropicBeta: r.Header.Get("anthropic-beta"),
			Body:          body,
		})
		rec.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

func (r *recordingServer) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

func TestDeliverClaudeRoutineSendsExpectedHeadersAndBody(t *testing.T) {
	store := newTestStore(t)
	srv, rec := newRecordingServer(t, 200)

	sub, _ := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, BearerToken: "sekret-token",
		Kind: webhookKindClaudeRoutine, Headers: map[string]string{}, Enabled: true,
	})

	d := testDispatcher(store)
	status, err := d.deliverOnce(sub, []WebhookEvent{testEvent(sub.ChatJID)})
	if err != nil || status != 200 {
		t.Fatalf("deliverOnce: status=%d err=%v", status, err)
	}

	if rec.count() != 1 {
		t.Fatalf("got %d requests, want 1", rec.count())
	}
	got := rec.requests[0]
	if got.Authorization != "Bearer sekret-token" {
		t.Fatalf("Authorization = %q", got.Authorization)
	}
	if got.AnthropicVer != "2023-06-01" {
		t.Fatalf("anthropic-version = %q", got.AnthropicVer)
	}
	if got.AnthropicBeta != "experimental-cc-routine-2026-04-01" {
		t.Fatalf("anthropic-beta = %q", got.AnthropicBeta)
	}
	if _, ok := got.Body["text"].(string); !ok {
		t.Fatalf("body missing text field: %+v", got.Body)
	}
	if _, ok := got.Body["events"]; !ok {
		t.Fatalf("body missing events field: %+v", got.Body)
	}
}

func TestDeliverGenericOmitsAnthropicHeaders(t *testing.T) {
	store := newTestStore(t)
	srv, rec := newRecordingServer(t, 200)

	sub, _ := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true,
	})

	d := testDispatcher(store)
	if _, err := d.deliverOnce(sub, []WebhookEvent{testEvent(sub.ChatJID)}); err != nil {
		t.Fatalf("deliverOnce: %v", err)
	}
	got := rec.requests[0]
	if got.AnthropicVer != "" || got.AnthropicBeta != "" {
		t.Fatalf("generic kind should omit anthropic headers, got %+v", got)
	}
}

func TestFromMeFiltering(t *testing.T) {
	store := newTestStore(t)
	srv, rec := newRecordingServer(t, 200)

	store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true, IncludeFromMe: false,
	})

	d := testDispatcher(store)
	event := testEvent("111@s.whatsapp.net")
	event.IsFromMe = true
	d.dispatch(event)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if rec.count() != 0 {
		t.Fatalf("expected no requests for from-me event, got %d", rec.count())
	}
}

func TestDebounceCoalescesEvents(t *testing.T) {
	store := newTestStore(t)
	srv, rec := newRecordingServer(t, 200)

	store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true, DebounceSeconds: 1,
	})

	d := testDispatcher(store)
	d.dispatch(testEvent("111@s.whatsapp.net"))
	time.Sleep(50 * time.Millisecond)
	d.dispatch(testEvent("111@s.whatsapp.net"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && rec.count() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // let any (incorrect) second POST land

	if rec.count() != 1 {
		t.Fatalf("got %d requests, want exactly 1 coalesced request", rec.count())
	}
	events, ok := rec.requests[0].Body["events"].([]any)
	if !ok || len(events) != 2 {
		t.Fatalf("expected 2 coalesced events in body, got %+v", rec.requests[0].Body["events"])
	}
}

// --- Retry / auto-disable policy ---

func TestFourOhFourDoesNotDisableOnFirstFailure(t *testing.T) {
	store := newTestStore(t)
	srv, _ := newRecordingServer(t, 404)

	sub, _ := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true,
	})

	d := testDispatcher(store)
	status, err := d.deliverOnce(sub, []WebhookEvent{testEvent(sub.ChatJID)})
	if err == nil || status != 404 {
		t.Fatalf("expected 404 failure, got status=%d err=%v", status, err)
	}

	got, _, _ := store.GetWebhookSubscription(sub.ID)
	if !got.Enabled {
		t.Fatalf("a single 404 must not disable the subscription")
	}
	if got.ConsecutiveFailures != 1 {
		t.Fatalf("ConsecutiveFailures = %d, want 1", got.ConsecutiveFailures)
	}
}

func TestThreeConsecutiveFailuresDisable(t *testing.T) {
	store := newTestStore(t)
	srv, _ := newRecordingServer(t, 500)

	sub, _ := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true,
	})

	d := testDispatcher(store)
	for i := 0; i < webhookMaxConsecutiveFail; i++ {
		d.deliverOnce(sub, []WebhookEvent{testEvent(sub.ChatJID)})
		d.clearCooldown(sub.ID) // simulate the cooldown elapsing before the next attempt
		sub, _, _ = store.GetWebhookSubscription(sub.ID)
	}

	if sub.Enabled {
		t.Fatalf("expected subscription disabled after %d consecutive failures", webhookMaxConsecutiveFail)
	}
	if sub.DisabledReason == "" {
		t.Fatalf("expected a disabled_reason to be recorded")
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	store := newTestStore(t)
	failing, _ := newRecordingServer(t, 500)

	sub, _ := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: failing.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true,
	})

	d := testDispatcher(store)
	d.deliverOnce(sub, []WebhookEvent{testEvent(sub.ChatJID)})
	sub, _, _ = store.GetWebhookSubscription(sub.ID)
	if sub.ConsecutiveFailures != 1 {
		t.Fatalf("ConsecutiveFailures = %d, want 1", sub.ConsecutiveFailures)
	}

	// Point the same subscription at a succeeding server and retry.
	ok, _ := newRecordingServer(t, 200)
	sub.URL = ok.URL
	d.deliverOnce(sub, []WebhookEvent{testEvent(sub.ChatJID)})

	sub, _, _ = store.GetWebhookSubscription(sub.ID)
	if sub.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d, want reset to 0 after success", sub.ConsecutiveFailures)
	}
}

func TestCooldownHoldsEventsUntilElapsed(t *testing.T) {
	store := newTestStore(t)
	srv, rec := newRecordingServer(t, 500)

	orig := webhookCooldownDuration
	webhookCooldownDuration = 300 * time.Millisecond
	t.Cleanup(func() { webhookCooldownDuration = orig })

	store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true,
	})

	d := testDispatcher(store)
	d.dispatch(testEvent("111@s.whatsapp.net"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && rec.count() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if rec.count() != 1 {
		t.Fatalf("expected first attempt to fire immediately, got %d", rec.count())
	}

	// A second event arriving right after the failure must not cause an
	// immediate second POST: it should be held for the cooldown.
	d.dispatch(testEvent("111@s.whatsapp.net"))
	time.Sleep(100 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("expected event to be held during cooldown, got %d requests", rec.count())
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && rec.count() < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	if rec.count() != 2 {
		t.Fatalf("expected cooldown-held event to fire as the next attempt, got %d", rec.count())
	}
}

func TestRateCapDropsExtraDelivery(t *testing.T) {
	store := newTestStore(t)
	srv, rec := newRecordingServer(t, 200)

	sub, _ := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true, MaxPerHour: 1,
	})

	d := testDispatcher(store)
	status1, err1 := d.attemptDelivery(sub, []WebhookEvent{testEvent(sub.ChatJID)}, true)
	if err1 != nil || status1 != 200 {
		t.Fatalf("first delivery should succeed: status=%d err=%v", status1, err1)
	}
	status2, err2 := d.attemptDelivery(sub, []WebhookEvent{testEvent(sub.ChatJID)}, true)
	if err2 != nil || status2 != 0 {
		t.Fatalf("second delivery should be dropped by rate cap: status=%d err=%v", status2, err2)
	}
	if rec.count() != 1 {
		t.Fatalf("got %d requests, want exactly 1 (rate-capped)", rec.count())
	}
}

func TestEnableRevivesSubscription(t *testing.T) {
	store := newTestStore(t)
	sub, _ := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: "https://example.com", Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true,
	})
	if err := store.disableWebhookSubscription(sub.ID, "3 consecutive failures, last: http 500"); err != nil {
		t.Fatalf("disableWebhookSubscription: %v", err)
	}
	store.incrementWebhookFailures(sub.ID)

	found, err := store.enableWebhookSubscription(sub.ID)
	if err != nil || !found {
		t.Fatalf("enableWebhookSubscription: found=%v err=%v", found, err)
	}

	got, _, _ := store.GetWebhookSubscription(sub.ID)
	if !got.Enabled {
		t.Fatalf("expected subscription re-enabled")
	}
	if got.DisabledReason != "" {
		t.Fatalf("expected disabled_reason cleared, got %q", got.DisabledReason)
	}
	if got.ConsecutiveFailures != 0 {
		t.Fatalf("expected consecutive_failures reset, got %d", got.ConsecutiveFailures)
	}
}

// --- REST masking ---

func TestListWebhooksHandlerMasksToken(t *testing.T) {
	store := newTestStore(t)
	store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: "https://example.com", BearerToken: "abcd1234", Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true,
	})

	mux := http.NewServeMux()
	registerWebhookRoutes(mux, store, testDispatcher(store), waLog.Noop)

	req := httptest.NewRequest(http.MethodGet, "/api/webhooks", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var views []WebhookSubscriptionView
	if err := json.NewDecoder(w.Body).Decode(&views); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(views) != 1 {
		t.Fatalf("got %d webhooks, want 1", len(views))
	}
	if views[0].BearerToken != "" {
		t.Fatalf("BearerToken should be masked, got %q", views[0].BearerToken)
	}
	if views[0].BearerTokenHint != "1234" {
		t.Fatalf("BearerTokenHint = %q, want %q", views[0].BearerTokenHint, "1234")
	}
}

func TestCreateAndDeleteWebhookHandlers(t *testing.T) {
	store := newTestStore(t)
	mux := http.NewServeMux()
	registerWebhookRoutes(mux, store, testDispatcher(store), waLog.Noop)

	body := `{"chat_jid":"111@s.whatsapp.net","url":"https://example.com/hook","bearer_token":"tok","kind":"generic"}`
	req := httptest.NewRequest(http.MethodPost, "/api/webhooks", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	var created WebhookSubscriptionView
	json.NewDecoder(w.Body).Decode(&created)

	delReq := httptest.NewRequest(http.MethodDelete, "/api/webhooks/"+strconv.Itoa(created.ID), nil)
	delW := httptest.NewRecorder()
	mux.ServeHTTP(delW, delReq)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d", delW.Code)
	}

	delReq2 := httptest.NewRequest(http.MethodDelete, "/api/webhooks/"+strconv.Itoa(created.ID), nil)
	delW2 := httptest.NewRecorder()
	mux.ServeHTTP(delW2, delReq2)
	if delW2.Code != http.StatusNotFound {
		t.Fatalf("second delete status = %d, want 404", delW2.Code)
	}
}

func TestFailuresDuringCooldownCountOnce(t *testing.T) {
	store := newTestStore(t)
	srv, _ := newRecordingServer(t, 500)

	sub, _ := store.AddWebhookSubscription(WebhookSubscription{
		ChatJID: "111@s.whatsapp.net", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true,
	})

	d := testDispatcher(store)
	// A burst of failures with no cooldown elapsing in between is one outage.
	for i := 0; i < webhookMaxConsecutiveFail+1; i++ {
		d.deliverOnce(sub, []WebhookEvent{testEvent(sub.ChatJID)})
	}

	sub, _, _ = store.GetWebhookSubscription(sub.ID)
	if !sub.Enabled {
		t.Fatalf("burst of failures within one cooldown window must not disable the subscription")
	}
	if sub.ConsecutiveFailures != 1 {
		t.Fatalf("ConsecutiveFailures = %d, want 1", sub.ConsecutiveFailures)
	}
}
