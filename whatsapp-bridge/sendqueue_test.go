package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newTestQueue builds a queue whose gate never waits, so tests exercise the
// queueing rather than the clock.
func newTestQueue(t *testing.T, send func(context.Context, SendMessageRequest) (bool, string)) *sendQueue {
	t.Helper()
	q := newSendQueue(newTestGate(0), send)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)
	return q
}

func waitFor(t *testing.T, done <-chan SendResult) SendResult {
	t.Helper()
	select {
	case res := <-done:
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("send never completed")
		return SendResult{}
	}
}

func TestSubmitReturnsBeforeTheSendHappens(t *testing.T) {
	release := make(chan struct{})
	sent := make(chan string, 1)
	q := newTestQueue(t, func(_ context.Context, req SendMessageRequest) (bool, string) {
		<-release
		sent <- req.Recipient
		return true, "sent"
	})

	job, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
	if !ok {
		t.Fatal("submit refused an empty queue")
	}

	select {
	case r := <-sent:
		t.Fatalf("send to %s happened before submit returned", r)
	default:
	}

	close(release)
	if res := waitFor(t, job.done); !res.Success {
		t.Fatalf("send failed: %s", res.Message)
	}
	if got := <-sent; got != "447700900000" {
		t.Fatalf("sent to %q", got)
	}
}

func TestBlockingCallerGetsTheOutcome(t *testing.T) {
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) {
		return false, "no LID found"
	})
	job, _, _ := q.submit(SendMessageRequest{Recipient: "447700900000", Block: true}, func() {})
	res := waitFor(t, job.done)
	if res.Success || res.Message != "no LID found" {
		t.Fatalf("result = %+v, want the failure passed through", res)
	}
	if res.State != sendFailed {
		t.Fatalf("state = %q, want %q", res.State, sendFailed)
	}
}

func TestCleanupRunsAfterTheSendNotBefore(t *testing.T) {
	var mu sync.Mutex
	var order []string
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) {
		mu.Lock()
		order = append(order, "send")
		mu.Unlock()
		return true, "sent"
	})

	cleaned := make(chan struct{})
	job, _, _ := q.submit(SendMessageRequest{Recipient: "447700900000", MediaPath: "/tmp/upload"}, func() {
		mu.Lock()
		order = append(order, "cleanup")
		mu.Unlock()
		close(cleaned)
	})
	waitFor(t, job.done)
	<-cleaned

	mu.Lock()
	defer mu.Unlock()
	// An async handler returns long before this: deleting the upload on the way
	// out would pull the file out from under the send.
	if len(order) != 2 || order[0] != "send" || order[1] != "cleanup" {
		t.Fatalf("order = %v, want [send cleanup]", order)
	}
}

func TestFullQueueRefusesAndLeavesCleanupToTheCaller(t *testing.T) {
	t.Setenv("WHATSAPP_SEND_MAX_QUEUE_DEPTH", "2")
	block := make(chan struct{})
	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) {
		<-block
		return true, "sent"
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.run(ctx)

	// One job is taken by the worker and parked in the send; the buffer then
	// holds two more before submissions are refused.
	accepted := 0
	var refusedCleanup bool
	for i := 0; i < 10; i++ {
		if _, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {}); ok {
			accepted++
		} else {
			refusedCleanup = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(block)

	if !refusedCleanup {
		t.Fatal("queue never filled; submissions must be refused once the backlog is capped")
	}
	if accepted < 2 || accepted > 4 {
		t.Fatalf("accepted %d before refusing, want ~3 (buffer 2 plus the in-flight one)", accepted)
	}
}

func TestResultIsReadableByID(t *testing.T) {
	release := make(chan struct{})
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) {
		<-release
		return true, "Message sent to 447700900000"
	})
	job, _, _ := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})

	res, ok := q.result(job.id)
	if !ok || res.State != sendQueued {
		t.Fatalf("before sending: %+v ok=%v, want state %q", res, ok, sendQueued)
	}
	if res.QueuedAt.IsZero() {
		t.Fatal("queued_at not recorded")
	}

	close(release)
	waitFor(t, job.done)

	res, ok = q.result(job.id)
	if !ok || res.State != sendSent || !res.Success {
		t.Fatalf("after sending: %+v ok=%v, want a successful %q", res, ok, sendSent)
	}
	if res.QueuedAt.IsZero() || res.SentAt.Before(res.QueuedAt) {
		t.Fatalf("timestamps out of order: queued %v, sent %v", res.QueuedAt, res.SentAt)
	}
}

func TestUnknownIDIsNotFound(t *testing.T) {
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) { return true, "sent" })
	if _, ok := q.result("snd-nope"); ok {
		t.Fatal("an unknown id came back as a known send")
	}
}

func TestResultsAreBounded(t *testing.T) {
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) { return true, "sent" })
	var last *sendJob
	for i := 0; i < resultsKept+50; i++ {
		job, _, ok := q.submit(SendMessageRequest{Recipient: fmt.Sprint(i)}, func() {})
		if !ok {
			t.Fatalf("submit %d refused", i)
		}
		waitFor(t, job.done)
		last = job
	}
	q.mu.Lock()
	n := len(q.results)
	q.mu.Unlock()
	if n > resultsKept {
		t.Fatalf("kept %d results, cap is %d", n, resultsKept)
	}
	if _, ok := q.result(last.id); !ok {
		t.Fatal("the most recent result was evicted")
	}
}

func TestShutdownWhileHeldByTheRateLimitFailsTheJob(t *testing.T) {
	var mu sync.Mutex
	var sentTo []string
	q := newSendQueue(newTestGate(time.Hour), func(_ context.Context, req SendMessageRequest) (bool, string) {
		mu.Lock()
		sentTo = append(sentTo, req.Recipient)
		mu.Unlock()
		return true, "sent"
	})
	ctx, cancel := context.WithCancel(context.Background())
	go q.run(ctx)

	// The first job is due immediately; the second is held for an hour.
	first, _, _ := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
	held, _, _ := q.submit(SendMessageRequest{Recipient: "447700900001"}, func() {})
	waitFor(t, first.done)
	cancel()

	res := waitFor(t, held.done)
	if res.Success {
		t.Fatal("a job held at shutdown reported success")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sentTo) != 1 || sentTo[0] != "447700900000" {
		t.Fatalf("sent %v, want only the job that was already due", sentTo)
	}
}

func TestFastSendKeepsItsOutcomeNotQueued(t *testing.T) {
	// The worker can finish before submit returns; the bookkeeping must not
	// then walk a finished send back to "queued".
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) {
		return true, "sent"
	})
	for i := 0; i < 200; i++ {
		job, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
		if !ok {
			t.Fatalf("submit %d refused", i)
		}
		waitFor(t, job.done)
		res, _ := q.result(job.id)
		if res.State != sendSent {
			t.Fatalf("iteration %d: state %q after the send completed", i, res.State)
		}
	}
}

func TestRefusedSubmissionLeavesNoTrace(t *testing.T) {
	t.Setenv("WHATSAPP_SEND_MAX_QUEUE_DEPTH", "1")
	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) {
		return true, "sent"
	})
	// No worker: the buffer fills and stays full.
	if _, _, ok := q.submit(SendMessageRequest{Recipient: "1"}, func() {}); !ok {
		t.Fatal("first submit refused")
	}
	before := q.pending()
	if _, _, ok := q.submit(SendMessageRequest{Recipient: "2"}, func() {}); ok {
		t.Fatal("submit accepted past the queue depth")
	}
	if got := q.pending(); got != before {
		t.Fatalf("pending = %d after a refusal, want %d", got, before)
	}
	q.mu.Lock()
	n := len(q.results)
	q.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d results recorded, want only the accepted one", n)
	}
}

func TestShutdownFailsEverythingStillWaiting(t *testing.T) {
	t.Setenv("WHATSAPP_SEND_MAX_QUEUE_DEPTH", "10")
	block := make(chan struct{})
	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) {
		<-block
		return true, "sent"
	})
	ctx, cancel := context.WithCancel(context.Background())
	go q.run(ctx)

	var jobs []*sendJob
	cleaned := make(chan string, 10)
	for i := 0; i < 5; i++ {
		job, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() { cleaned <- "x" })
		if !ok {
			t.Fatalf("submit %d refused", i)
		}
		jobs = append(jobs, job)
	}

	cancel()
	close(block)

	// Nothing may be left holding a caller or silently dropped: every job ends
	// with a result, and every upload is cleaned up.
	for i, job := range jobs {
		res := waitFor(t, job.done)
		if res.State == sendQueued {
			t.Fatalf("job %d still reads as queued after shutdown", i)
		}
		if stored, _ := q.result(job.id); stored.State == sendQueued {
			t.Fatalf("job %d left recorded as queued; a client would wait forever", i)
		}
	}
	for range jobs {
		select {
		case <-cleaned:
		case <-time.After(5 * time.Second):
			t.Fatal("an upload was never cleaned up at shutdown")
		}
	}
}
