package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Two agents woken for the same message both try to send the same reply: the
// second submission with the key must not queue anything.
func TestIdempotencyKeySendsOnce(t *testing.T) {
	var sends int32
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) {
		atomic.AddInt32(&sends, 1)
		return true, "sent"
	})
	req := SendMessageRequest{Recipient: "447700900000", Message: "hi", IdempotencyKey: "reply-1"}

	job, _, dup, ok := q.submitOnce(req, func() {})
	if !ok || dup != nil {
		t.Fatalf("first submission: ok=%v dup=%+v, want queued", ok, dup)
	}
	// Still queued or already sent, either way a repeat is refused.
	if _, _, dup, ok := q.submitOnce(req, func() {}); !ok || dup == nil || dup.ID != job.id {
		t.Fatalf("repeat while queued: ok=%v dup=%+v, want the first submission", ok, dup)
	}
	waitFor(t, job.done)
	if _, _, dup, ok := q.submitOnce(req, func() {}); !ok || dup == nil || dup.ID != job.id || dup.State != sendSent {
		t.Fatalf("repeat after sending: ok=%v dup=%+v, want the sent submission", ok, dup)
	}
	if n := atomic.LoadInt32(&sends); n != 1 {
		t.Fatalf("sent %d times, want 1", n)
	}

	// A different key, or none, is an ordinary send.
	other, _, dup, ok := q.submitOnce(SendMessageRequest{Recipient: "447700900000", Message: "hi", IdempotencyKey: "reply-2"}, func() {})
	if !ok || dup != nil {
		t.Fatalf("different key: ok=%v dup=%+v, want queued", ok, dup)
	}
	waitFor(t, other.done)
}

// Concurrent submissions with one key: exactly one is queued.
func TestIdempotencyKeyRaceQueuesOne(t *testing.T) {
	var sends int32
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) {
		atomic.AddInt32(&sends, 1)
		return true, "sent"
	})
	var wg sync.WaitGroup
	var queued int32
	jobs := make(chan *sendJob, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, _, dup, ok := q.submitOnce(SendMessageRequest{Recipient: "447700900000", IdempotencyKey: "same"}, func() {})
			if ok && dup == nil {
				atomic.AddInt32(&queued, 1)
				jobs <- job
			}
		}()
	}
	wg.Wait()
	close(jobs)
	if queued != 1 {
		t.Fatalf("%d submissions queued, want 1", queued)
	}
	for job := range jobs {
		waitFor(t, job.done)
	}
	if sends != 1 {
		t.Fatalf("sent %d times, want 1", sends)
	}
}

// A failed send does not use the key up: the retry goes through.
func TestIdempotencyKeyAllowsRetryAfterFailure(t *testing.T) {
	var calls int32
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return false, "not connected"
		}
		return true, "sent"
	})
	req := SendMessageRequest{Recipient: "447700900000", IdempotencyKey: "k"}
	first, _, _, _ := q.submitOnce(req, func() {})
	if res := waitFor(t, first.done); res.Success {
		t.Fatal("first send should have failed")
	}
	retry, _, dup, ok := q.submitOnce(req, func() {})
	if !ok || dup != nil {
		t.Fatalf("retry after failure: ok=%v dup=%+v, want queued", ok, dup)
	}
	if res := waitFor(t, retry.done); !res.Success {
		t.Fatalf("retry failed: %s", res.Message)
	}
	if _, _, dup, _ := q.submitOnce(req, func() {}); dup == nil || dup.ID != retry.id {
		t.Fatalf("after the successful retry, dup = %+v, want the retry", dup)
	}
}

// The key survives a bridge restart: the redeploy must not reopen the door.
func TestIdempotencyKeySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	q := persistentTestQueue(t, dir, newTestGate(0), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go q.run(ctx)
	job, _, _, ok := q.submitOnce(SendMessageRequest{Recipient: "447700900000", IdempotencyKey: "k"}, func() {})
	if !ok {
		t.Fatal("submit refused")
	}
	waitFor(t, job.done)
	cancel()

	q2, _ := reopen(t, dir, newTestGate(0), nil, nil)
	_, _, dup, ok := q2.submitOnce(SendMessageRequest{Recipient: "447700900000", IdempotencyKey: "k"}, func() {})
	if !ok || dup == nil || dup.ID != job.id || dup.State != sendSent {
		t.Fatalf("after restart: ok=%v dup=%+v, want the original sent submission", ok, dup)
	}
}

// A sendqueue.db from before the column existed is migrated in place.
func TestSendQueueStoreMigratesIdempotencyKeyColumn(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(dir, "sendqueue.db"))
	if err != nil {
		t.Fatal(err)
	}
	old := strings.Replace(sendQueueSchema, ",\n\tidempotency_key TEXT NOT NULL DEFAULT ''", "", 1)
	if old == sendQueueSchema {
		t.Fatal("test could not strip the column from the schema")
	}
	if _, err := db.Exec(old); err != nil {
		t.Fatal(err)
	}
	db.Close()

	store, err := openSendQueueStore(dir)
	if err != nil {
		t.Fatalf("opening an old-schema store: %v", err)
	}
	defer store.Close()
	if _, found, err := store.findByKey("k"); err != nil || found {
		t.Fatalf("findByKey on migrated store: found=%v err=%v", found, err)
	}
}

// Over HTTP: the repeat is a 200 carrying duplicate and the first id.
func TestSendHandlerReportsDuplicate(t *testing.T) {
	var sends int32
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) {
		atomic.AddInt32(&sends, 1)
		return true, "sent"
	})
	post := func() SendMessageResponse {
		r := httptest.NewRequest("POST", "/api/send", strings.NewReader(`{"recipient":"447700900000","message":"hi","idempotency_key":"reply-1"}`))
		w := httptest.NewRecorder()
		sendHandler(q, t.TempDir(), alwaysReady, false)(w, r)
		if w.Code != http.StatusAccepted && w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		var resp SendMessageResponse
		json.NewDecoder(w.Body).Decode(&resp)
		return resp
	}
	first := post()
	second := post()
	if first.Duplicate || !first.Success {
		t.Fatalf("first response = %+v", first)
	}
	if !second.Duplicate || !second.Success || second.ID != first.ID {
		t.Fatalf("second response = %+v, want a duplicate of %s", second, first.ID)
	}
}
