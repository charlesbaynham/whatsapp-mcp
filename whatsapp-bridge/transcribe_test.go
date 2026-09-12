package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func newTranscribeFixture(t *testing.T, run func(ctx context.Context, path string) (string, error)) (*MessageStore, *Publisher, *Transcriber) {
	t.Helper()
	store := newTestStore(t)
	pub := testPublisher(store)
	tr := NewTranscriber(TranscriberConfig{Enabled: true, Timeout: 200 * time.Millisecond}, store, pub, waLog.Noop)
	tr.fetch = func(ctx context.Context, chatJID, messageID string) (string, error) {
		return "/fake/" + messageID + ".ogg", nil
	}
	tr.run = run
	ts := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	if err := store.StoreChat("c@s.whatsapp.net", "Carol", ts); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreMessage("v1", "c@s.whatsapp.net", "447700900000", "", ts, false, "audio", "audio_1.ogg", "u", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	return store, pub, tr
}

func lastEvent(t *testing.T, store *MessageStore, since int64) (Event, WebhookEvent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ev, ok := store.waitForEvent(ctx, since)
	if !ok {
		t.Fatal("no event published")
	}
	var msg WebhookEvent
	if err := json.Unmarshal(ev.Data, &msg); err != nil {
		t.Fatal(err)
	}
	return ev, msg
}

func TestTranscribeGatesPublicationUntilDone(t *testing.T) {
	store, _, tr := newTranscribeFixture(t, func(ctx context.Context, path string) (string, error) {
		return " hello\n [MUSIC]\n world ", nil
	})
	if err := store.markTranscriptionPending("c@s.whatsapp.net", "v1"); err != nil {
		t.Fatal(err)
	}
	if evs, _ := store.ListEvents(0, 10); len(evs) != 0 {
		t.Fatal("nothing must be published before transcription")
	}
	tr.Start()
	ev, msg := lastEvent(t, store, 0)
	if ev.Type != eventMessageNew || msg.Transcript != "hello world" || msg.TranscriptionStatus != transcriptionOK || !msg.HasMedia || msg.MediaType != "audio" {
		t.Errorf("published %s / %+v", ev.Type, msg)
	}
	views, _ := store.ListMessages(ListMessagesParams{ChatJID: "c@s.whatsapp.net"})
	if len(views) != 1 || views[0].Transcript != "hello world" || views[0].TranscriptionStatus != transcriptionOK {
		t.Errorf("read API view: %+v", views)
	}
}

func TestTranscribeFailureStillPublishes(t *testing.T) {
	store, _, tr := newTranscribeFixture(t, func(ctx context.Context, path string) (string, error) {
		return "", errors.New("whisper exploded")
	})
	tr.Start()
	tr.Enqueue(transcribeJob{chatJID: "c@s.whatsapp.net", messageID: "v1"})
	_, msg := lastEvent(t, store, 0)
	if msg.TranscriptionStatus != transcriptionFailed || msg.Transcript != "" {
		t.Errorf("%+v", msg)
	}
}

func TestTranscribeTimeoutStillPublishes(t *testing.T) {
	store, _, tr := newTranscribeFixture(t, func(ctx context.Context, path string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	tr.Start()
	tr.Enqueue(transcribeJob{chatJID: "c@s.whatsapp.net", messageID: "v1"})
	_, msg := lastEvent(t, store, 0)
	if msg.TranscriptionStatus != transcriptionTimeout {
		t.Errorf("status = %q, want timeout", msg.TranscriptionStatus)
	}
}

func TestTranscribeQueueFullFailsOpen(t *testing.T) {
	store, _, tr := newTranscribeFixture(t, nil)
	// Never start the worker; fill the queue, then one more must publish immediately as failed.
	for i := 0; i < transcribeQueueSize; i++ {
		tr.queue <- transcribeJob{chatJID: "x", messageID: "y"}
	}
	tr.Enqueue(transcribeJob{chatJID: "c@s.whatsapp.net", messageID: "v1"})
	_, msg := lastEvent(t, store, 0)
	if msg.TranscriptionStatus != transcriptionFailed {
		t.Errorf("%+v", msg)
	}
	_ = store
}

func TestBackfillRoutePublishesUpdated(t *testing.T) {
	store, pub, tr := newTranscribeFixture(t, func(ctx context.Context, path string) (string, error) { return "later", nil })
	// Simulate a note that was published without a transcript earlier.
	store.setTranscription("c@s.whatsapp.net", "v1", "", transcriptionDisabled)
	ev, _, _ := store.messageEvent("c@s.whatsapp.net", "v1")
	pub.PublishMessage(ev)
	tr.Start()

	mux := http.NewServeMux()
	registerTranscribeRoutes(mux, store, tr)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/api/messages/c@s.whatsapp.net/v1/transcribe", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status %d", resp.StatusCode)
	}
	upd, msg := lastEvent(t, store, 1)
	if upd.Type != eventMessageUpdated || msg.Transcript != "later" {
		t.Errorf("%s %+v", upd.Type, msg)
	}

	resp, _ = http.Post(srv.URL+"/api/messages/c@s.whatsapp.net/nope/transcribe", "application/json", nil)
	if resp.StatusCode != 404 {
		t.Errorf("unknown message -> %d", resp.StatusCode)
	}
	store.StoreMessage("t1", "c@s.whatsapp.net", "447700900000", "text", time.Now(), false, "", "", "", nil, nil, nil, 0)
	resp, _ = http.Post(srv.URL+"/api/messages/c@s.whatsapp.net/t1/transcribe", "application/json", nil)
	if resp.StatusCode != 400 {
		t.Errorf("text message -> %d", resp.StatusCode)
	}

	off := NewTranscriber(TranscriberConfig{Enabled: false}, store, pub, waLog.Noop)
	mux2 := http.NewServeMux()
	registerTranscribeRoutes(mux2, store, off)
	srv2 := httptest.NewServer(mux2)
	defer srv2.Close()
	resp, _ = http.Post(srv2.URL+"/api/messages/c@s.whatsapp.net/v1/transcribe", "application/json", nil)
	if resp.StatusCode != 503 {
		t.Errorf("disabled -> %d", resp.StatusCode)
	}
}

func TestTranscribeRestartRequeuesPending(t *testing.T) {
	store, _, tr := newTranscribeFixture(t, func(ctx context.Context, path string) (string, error) { return "recovered", nil })
	store.markTranscriptionPending("c@s.whatsapp.net", "v1")
	pending, err := store.pendingTranscriptions()
	if err != nil || len(pending) != 1 || pending[0].messageID != "v1" {
		t.Fatalf("pending = %+v err=%v", pending, err)
	}
	tr.Start() // sweeps pending rows
	_, msg := lastEvent(t, store, 0)
	if msg.Transcript != "recovered" {
		t.Errorf("%+v", msg)
	}
	if left, _ := store.pendingTranscriptions(); len(left) != 0 {
		t.Errorf("still pending: %+v", left)
	}
}

func TestReleasePendingWhenDisabled(t *testing.T) {
	store, pub, _ := newTranscribeFixture(t, nil)
	store.markTranscriptionPending("c@s.whatsapp.net", "v1")
	off := NewTranscriber(TranscriberConfig{Enabled: false}, store, pub, waLog.Noop)
	off.ReleasePending()
	_, msg := lastEvent(t, store, 0)
	if msg.TranscriptionStatus != transcriptionDisabled {
		t.Errorf("%+v", msg)
	}
	if left, _ := store.pendingTranscriptions(); len(left) != 0 {
		t.Errorf("still pending: %+v", left)
	}
}

func TestEnsureColumnIsIdempotent(t *testing.T) {
	store := newTestStore(t)
	for i := 0; i < 2; i++ {
		if err := ensureColumn(store.db, "messages", "transcript", "TEXT"); err != nil {
			t.Fatal(err)
		}
	}
	if err := ensureColumn(store.db, "chats", "extra_col", "TEXT"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`UPDATE chats SET extra_col = 'x'`); err != nil {
		t.Fatal(err)
	}
}

func TestCleanTranscript(t *testing.T) {
	got := cleanTranscript("\n [BLANK_AUDIO]\n Hi there.\n(laughs)\n How are you?\n")
	if got != "Hi there. How are you?" {
		t.Errorf("got %q", got)
	}
	if !strings.Contains(cleanTranscript("one\ntwo"), "one two") {
		t.Error("lines not joined")
	}
}
