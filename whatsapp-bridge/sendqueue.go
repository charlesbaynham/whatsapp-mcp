package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

const (
	defaultQueueDepth = 100

	// The worker owns the send, so it cannot inherit an HTTP request's context:
	// that one is cancelled the moment the handler returns, which for an async
	// submission is before the message has gone anywhere.
	sendTimeout = 5 * time.Minute

	// Results are kept only so a client can ask how its submission went; the
	// message store is the durable record.
	resultsKept = 500
)

type sendState string

const (
	sendQueued sendState = "queued"
	sendSent   sendState = "sent"
	sendFailed sendState = "failed"
)

// SendResult is the outcome of one submission, readable at GET /api/send/{id}.
type SendResult struct {
	ID       string    `json:"id"`
	State    sendState `json:"state"`
	Success  bool      `json:"success"`
	Message  string    `json:"message"`
	QueuedAt time.Time `json:"queued_at"`
	SentAt   time.Time `json:"sent_at,omitempty"`
}

type sendJob struct {
	id      string
	req     SendMessageRequest
	cleanup func() // removes an uploaded temp file; the job owns it once queued
	done    chan SendResult
}

// sendQueue serialises outbound sends through one worker, spacing them with the
// gate. Submissions are accepted and acknowledged immediately; a caller that
// wants the outcome waits on the returned channel instead.
type sendQueue struct {
	jobs chan *sendJob
	gate *sendGate
	send func(ctx context.Context, req SendMessageRequest) (bool, string)

	mu      sync.Mutex
	results map[string]SendResult
	order   []string
	depth   int
}

func newSendQueue(gate *sendGate, send func(context.Context, SendMessageRequest) (bool, string)) *sendQueue {
	depth := envPositiveInt("WHATSAPP_SEND_MAX_QUEUE_DEPTH", defaultQueueDepth)
	return &sendQueue{
		jobs:    make(chan *sendJob, depth),
		gate:    gate,
		send:    send,
		results: make(map[string]SendResult, resultsKept),
	}
}

// run drives the queue until ctx is cancelled. One goroutine, so sends never
// overlap and the gate needs no lock.
func (q *sendQueue) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-q.jobs:
			q.dispatch(ctx, job)
		}
	}
}

func (q *sendQueue) dispatch(ctx context.Context, job *sendJob) {
	defer job.cleanup()

	if wait := time.Until(q.gate.due(time.Now())); wait > 0 {
		fmt.Printf("Holding send %s for %s (rate limit)\n", job.id, wait.Round(time.Second))
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			q.finish(job, false, "bridge shutting down before the send left the queue")
			return
		case <-t.C:
		}
	}

	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	success, message := q.send(sendCtx, job.req)
	fmt.Println("Message sent", success, message)
	q.finish(job, success, message)
}

// submit queues a job, returning its result channel. ok is false when the queue
// is full, in which case nothing is queued and the caller still owns cleanup.
func (q *sendQueue) submit(req SendMessageRequest, cleanup func()) (job *sendJob, ok bool) {
	job = &sendJob{id: newSendID(), req: req, cleanup: cleanup, done: make(chan SendResult, 1)}

	// Book the job in before handing it to the worker: a send can complete
	// before this function returns, and its outcome must not be overwritten by
	// this one's "queued".
	q.mu.Lock()
	q.depth++
	q.mu.Unlock()
	q.record(SendResult{ID: job.id, State: sendQueued, QueuedAt: time.Now()})

	select {
	case q.jobs <- job:
		return job, true
	default:
		q.forget(job.id)
		return nil, false
	}
}

func (q *sendQueue) finish(job *sendJob, success bool, message string) {
	state := sendFailed
	if success {
		state = sendSent
	}
	res := SendResult{
		ID: job.id, State: state, Success: success, Message: message,
		QueuedAt: q.queuedAt(job.id), SentAt: time.Now(),
	}
	q.record(res)
	q.mu.Lock()
	q.depth--
	q.mu.Unlock()
	job.done <- res
}

func (q *sendQueue) queuedAt(id string) time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.results[id].QueuedAt
}

// record stores a result, evicting the oldest once resultsKept is reached. A
// finished send is never walked back to "queued", whatever order the goroutines
// arrive in.
func (q *sendQueue) record(res SendResult) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if prev, seen := q.results[res.ID]; seen {
		if res.State == sendQueued && prev.State != sendQueued {
			return
		}
	} else {
		q.order = append(q.order, res.ID)
		for len(q.order) > resultsKept {
			delete(q.results, q.order[0])
			q.order = q.order[1:]
		}
	}
	q.results[res.ID] = res
}

// forget drops a booking the queue could not accept.
func (q *sendQueue) forget(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.depth--
	delete(q.results, id)
	for i, known := range q.order {
		if known == id {
			q.order = append(q.order[:i], q.order[i+1:]...)
			break
		}
	}
}

func (q *sendQueue) result(id string) (SendResult, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	res, ok := q.results[id]
	return res, ok
}

func (q *sendQueue) pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.depth
}

func newSendID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("snd-%d", time.Now().UnixNano())
	}
	return "snd-" + hex.EncodeToString(b[:])
}
