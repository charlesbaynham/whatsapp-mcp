package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func testPublisher(store *MessageStore) *Publisher {
	return NewPublisher(store, nil, waLog.Noop)
}

func TestEventLogAppendAndList(t *testing.T) {
	store := newTestStore(t)
	pub := testPublisher(store)
	pub.PublishMessage(WebhookEvent{MessageID: "m1", ChatJID: "a@s.whatsapp.net", Content: "hi"})
	pub.PublishChatRead("a@s.whatsapp.net", time.Now(), "api")
	pub.PublishMessage(WebhookEvent{MessageID: "m2", ChatJID: "b@s.whatsapp.net", Content: "me", IsFromMe: true})

	evs, err := store.ListEvents(0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 3 || evs[0].ID != 1 || evs[2].ID != 3 {
		t.Fatalf("events: %+v", evs)
	}
	if evs[0].Type != eventMessageNew || evs[1].Type != eventChatRead || !evs[2].IsFromMe {
		t.Errorf("types/flags: %+v", evs)
	}
	var msg WebhookEvent
	if err := json.Unmarshal(evs[0].Data, &msg); err != nil || msg.Content != "hi" {
		t.Errorf("payload: %s %v", evs[0].Data, err)
	}
	after, _ := store.ListEvents(1, 10)
	if len(after) != 2 || after[0].ID != 2 {
		t.Errorf("since=1: %+v", after)
	}
	latest, _ := store.LatestEventID()
	if latest != 3 {
		t.Errorf("latest = %d", latest)
	}
}

func TestEventFilter(t *testing.T) {
	f := eventFilter{chatJID: "a@s.whatsapp.net", types: map[string]bool{eventMessageNew: true}}
	if !f.allows(Event{Type: eventMessageNew, ChatJID: "a@s.whatsapp.net"}) {
		t.Error("matching event refused")
	}
	if f.allows(Event{Type: eventMessageNew, ChatJID: "b@s.whatsapp.net"}) {
		t.Error("other chat allowed")
	}
	if f.allows(Event{Type: eventChatRead, ChatJID: "a@s.whatsapp.net"}) {
		t.Error("other type allowed")
	}
	if f.allows(Event{Type: eventMessageNew, ChatJID: "a@s.whatsapp.net", IsFromMe: true}) {
		t.Error("from-me allowed by default")
	}
	// Chat-less events (bridge.status) pass a chat filter.
	if !(eventFilter{chatJID: "a@s.whatsapp.net"}).allows(Event{Type: eventBridgeStatus}) {
		t.Error("bridge.status blocked by chat filter")
	}
}

// readFrames reads SSE frames off a streaming response until n have arrived or the deadline passes.
func readFrames(t *testing.T, body *bufio.Reader, n int, deadline time.Duration) []Event {
	t.Helper()
	var out []Event
	done := make(chan struct{})
	go func() {
		defer close(done)
		for len(out) < n {
			line, err := body.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var ev Event
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev); err != nil {
				t.Errorf("bad frame %q: %v", line, err)
				return
			}
			out = append(out, ev)
		}
	}()
	select {
	case <-done:
	case <-time.After(deadline):
		t.Fatalf("timed out waiting for %d frames, got %d", n, len(out))
	}
	return out
}

func TestEventsStreamReplayThenLive(t *testing.T) {
	store := newTestStore(t)
	pub := testPublisher(store)
	pub.PublishMessage(WebhookEvent{MessageID: "m1", ChatJID: "a@s.whatsapp.net", Content: "one"})
	pub.PublishMessage(WebhookEvent{MessageID: "m2", ChatJID: "a@s.whatsapp.net", Content: "two"})

	mux := http.NewServeMux()
	registerEventRoutes(mux, store, pub)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/api/events", nil)
	req.Header.Set("Last-Event-ID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	body := bufio.NewReader(resp.Body)

	replay := readFrames(t, body, 1, 2*time.Second)
	if replay[0].ID != 2 || replay[0].MessageID != "m2" {
		t.Fatalf("replay after id 1: %+v", replay)
	}

	// Wait until the handler has subscribed before publishing live.
	for i := 0; i < 100 && pub.hub.count() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	pub.PublishMessage(WebhookEvent{MessageID: "m3", ChatJID: "a@s.whatsapp.net", Content: "three"})
	live := readFrames(t, body, 1, 2*time.Second)
	if live[0].ID != 3 || live[0].MessageID != "m3" {
		t.Fatalf("live: %+v", live)
	}
	cancel()
}

func TestEventsStreamFilters(t *testing.T) {
	store := newTestStore(t)
	pub := testPublisher(store)
	pub.PublishMessage(WebhookEvent{MessageID: "m1", ChatJID: "a@s.whatsapp.net"})
	pub.PublishMessage(WebhookEvent{MessageID: "m2", ChatJID: "b@s.whatsapp.net"})
	pub.PublishMessage(WebhookEvent{MessageID: "m3", ChatJID: "b@s.whatsapp.net", IsFromMe: true})
	pub.PublishChatRead("b@s.whatsapp.net", time.Now(), "api")

	mux := http.NewServeMux()
	registerEventRoutes(mux, store, pub)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/api/events?since=0&chat_jid=b@s.whatsapp.net&types=message.new&include_from_me=true", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	frames := readFrames(t, bufio.NewReader(resp.Body), 2, 2*time.Second)
	if frames[0].MessageID != "m2" || frames[1].MessageID != "m3" {
		t.Errorf("filtered frames: %+v", frames)
	}

	bad, _ := http.Get(srv.URL + "/api/events?since=-1")
	if bad.StatusCode != 400 {
		t.Errorf("since=-1 -> %d", bad.StatusCode)
	}
}

func TestSlowSubscriberIsDroppedNotBlocked(t *testing.T) {
	hub := newSSEHub()
	ch, unsub := hub.subscribe()
	defer unsub()
	for i := 0; i < sseSubscriberBuf+5; i++ {
		hub.broadcast(Event{ID: int64(i + 1)})
	}
	if hub.count() != 0 {
		t.Fatal("overflowing subscriber still registered")
	}
	n := 0
	for range ch {
		n++
	}
	if n != sseSubscriberBuf {
		t.Errorf("received %d buffered events, want %d then close", n, sseSubscriberBuf)
	}
}

func TestPublisherFeedsWebhookDispatcher(t *testing.T) {
	store := newTestStore(t)
	srv, rec := newRecordingServer(t, 200)
	defer srv.Close()
	if _, err := store.AddWebhookSubscription(WebhookSubscription{ChatJID: "*", URL: srv.URL, Kind: webhookKindGeneric, Headers: map[string]string{}, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	pub := NewPublisher(store, testDispatcher(store), waLog.Noop)
	pub.PublishMessage(WebhookEvent{MessageID: "m1", ChatJID: "a@s.whatsapp.net", Content: "hi", MediaType: "audio", Transcript: "hello world", DurationSeconds: 65})
	for i := 0; i < 200 && rec.count() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if rec.count() != 1 {
		t.Fatal("webhook not delivered")
	}
	rec.mu.Lock()
	text, _ := rec.requests[0].Body["text"].(string)
	rec.mu.Unlock()
	if !strings.Contains(text, "[voice note, 1:05] hello world") {
		t.Errorf("webhook body: %s", text)
	}
	evs, _ := store.ListEvents(0, 10)
	if len(evs) != 1 {
		t.Errorf("event log has %d rows", len(evs))
	}
}
