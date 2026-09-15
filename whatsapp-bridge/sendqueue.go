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

	// Never r.Context(): that is cancelled when the handler returns, which for
	// an async submission is before the message has gone anywhere.
	sendTimeout = 5 * time.Minute

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
	ID      string    `json:"id"`
	State   sendState `json:"state"`
	Success bool      `json:"success"`
	Message string    `json:"message"`
	// NewContact marks a send to someone this account has never messaged: it
	// waits in the new-contact stage before it is even on the main queue, so
	// "queued" can mean a good deal longer than usual.
	NewContact bool      `json:"new_contact,omitempty"`
	QueuedAt   time.Time `json:"queued_at"`
	SentAt     time.Time `json:"sent_at,omitzero"`
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
//
// A send to someone the account has never messaged does not join the main
// queue directly: it waits in newContacts first (see newContactStage), and is
// handed on to jobs — subject to the ordinary gate like everything else — only
// when that stage releases it.
type sendQueue struct {
	jobs        chan *sendJob
	gate        *sendGate
	newContacts *newContactStage // nil: first contacts are not held separately
	send        func(ctx context.Context, req SendMessageRequest) (bool, string)

	mu      sync.Mutex
	results map[string]SendResult
	order   []string
	depth   int // on the main queue, the in-flight send included
	held    int // in the new-contact stage
}

func newSendQueue(gate *sendGate, send func(context.Context, SendMessageRequest) (bool, string)) *sendQueue {
	return &sendQueue{
		jobs:    make(chan *sendJob, queueDepthFromEnv()),
		gate:    gate,
		send:    send,
		results: make(map[string]SendResult, resultsKept),
	}
}

// queueDepthFromEnv is how many submissions a queue accepts before refusing
// more. A backlog deeper than the results kept would evict a record while its
// message is still waiting, so /api/send/{id} would 404 on a live send; the
// main queue and the new-contact stage each hold this many, so they share
// the results budget.
func queueDepthFromEnv() int {
	return min(envPositiveInt("WHATSAPP_SEND_MAX_QUEUE_DEPTH", defaultQueueDepth), resultsKept/2)
}

// run drives the queue until ctx is cancelled: one worker goroutine for the
// main queue and one for the new-contact stage, so sends never overlap and
// each gate is advanced from a single place.
func (q *sendQueue) run(ctx context.Context) {
	if q.newContacts != nil {
		go q.newContacts.run(ctx, q)
	}
	for {
		select {
		case <-ctx.Done():
			q.drain()
			return
		case job := <-q.jobs:
			q.dispatch(ctx, job)
		}
	}
}

func (q *sendQueue) dispatch(ctx context.Context, job *sendJob) {
	defer job.cleanup()

	if ctx.Err() != nil {
		q.abandon(job)
		return
	}

	if wait := time.Until(q.gate.due(time.Now())); wait > 0 {
		fmt.Printf("Holding send %s for %s (rate limit)\n", job.id, wait.Round(time.Second))
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			q.abandon(job)
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

// drain fails everything still waiting when the bridge stops. Nothing here has
// been sent, and the queue is memory-only, so each one has to be named in the
// log: the alternative is a message that silently never went out.
func (q *sendQueue) drain() {
	for {
		select {
		case job := <-q.jobs:
			job.cleanup()
			q.abandon(job)
		default:
			return
		}
	}
}

func (q *sendQueue) abandon(job *sendJob) {
	fmt.Printf("Send %s to %s abandoned: bridge stopped before it left the queue\n", job.id, job.req.Recipient)
	q.finish(job, false, "bridge stopped before the send left the queue")
}

// queuePosition is where a submission landed and what that implies for the
// caller: how many sends are ahead of it (on the main queue, or in the
// new-contact stage when newContact is set, where ahead counts the other
// first contacts still waiting there) and a rough expected wait before it
// goes out, from the gates' means and whatever backlog and reach-out
// time-lock are in force at submission.
type queuePosition struct {
	ahead      int
	newContact bool
	wait       time.Duration
}

// submit queues a job, returning it and its position. ok is false when the
// queue is full, in which case nothing is queued and the caller still owns
// cleanup.
func (q *sendQueue) submit(req SendMessageRequest, cleanup func()) (job *sendJob, pos queuePosition, ok bool) {
	job = &sendJob{id: newSendID(), req: req, cleanup: cleanup, done: make(chan SendResult, 1)}

	pos.newContact = q.newContacts != nil && q.newContacts.holds(req.Recipient)

	// Book the job in before handing it to the worker: a send can complete
	// before this function returns, and its outcome must not be overwritten by
	// this one's "queued".
	q.mu.Lock()
	if pos.newContact {
		q.held++
		pos.ahead = q.held - 1
		pos.wait = q.newContacts.expectedWait(pos.ahead) + q.mainWaitLocked(q.depth)
	} else {
		q.depth++
		pos.ahead = q.depth - 1
		pos.wait = q.mainWaitLocked(pos.ahead)
	}
	q.mu.Unlock()
	q.record(SendResult{ID: job.id, State: sendQueued, NewContact: pos.newContact, QueuedAt: time.Now()})

	dest := q.jobs
	if pos.newContact {
		dest = q.newContacts.jobs
	}
	select {
	case dest <- job:
		return job, pos, true
	default:
		q.forget(job.id, pos.newContact)
		return nil, queuePosition{}, false
	}
}

// preview reports, without queueing anything, whether a send to recipient
// would be held as a new contact and roughly how long it would wait: what
// submit would book it at if called now.
func (q *sendQueue) preview(recipient string) (newContact bool, wait time.Duration) {
	if q.newContacts == nil || !q.newContacts.holds(recipient) {
		return false, 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return true, q.newContacts.expectedWait(q.held) + q.mainWaitLocked(q.depth)
}

// mainWaitLocked estimates how long a send behind `ahead` others on the main
// queue waits: until the gate's next slot, then one mean gap per send ahead
// of it. A send in flight has already used its slot, so this errs a gap long
// while one is out. Called with q.mu held.
func (q *sendQueue) mainWaitLocked(ahead int) time.Duration {
	if !q.gate.enabled() {
		return 0
	}
	return waitFrom(q.gate.peek()) + time.Duration(ahead)*q.gate.mean
}

// waitFrom is how far off an instant is, or zero once it has passed.
func waitFrom(t time.Time) time.Duration {
	if d := time.Until(t); d > 0 {
		return d
	}
	return 0
}

// promote moves a job the new-contact stage is done with onto the main
// queue's books, before it is actually handed over, so its result and the
// depth counts never disagree about where it is.
func (q *sendQueue) promote(job *sendJob) {
	q.mu.Lock()
	q.held--
	q.depth++
	q.mu.Unlock()
}

func (q *sendQueue) finish(job *sendJob, success bool, message string) {
	state := sendFailed
	if success {
		state = sendSent
	}
	booked := q.booking(job.id)
	res := SendResult{
		ID: job.id, State: state, Success: success, Message: message,
		NewContact: booked.NewContact, QueuedAt: booked.QueuedAt, SentAt: time.Now(),
	}
	q.record(res)
	q.mu.Lock()
	q.depth--
	q.mu.Unlock()
	job.done <- res
}

// booking is the record made when a job was submitted; the fields that
// describe the submission rather than its outcome carry over into the result.
func (q *sendQueue) booking(id string) SendResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.results[id]
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
func (q *sendQueue) forget(id string, newContact bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if newContact {
		q.held--
	} else {
		q.depth--
	}
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

// pending is how many submissions have not yet finished, wherever they are.
func (q *sendQueue) pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.depth + q.held
}

// sendQueueStatus is the JSON shape exposed under send_queue in /api/status.
type sendQueueStatus struct {
	// Pending is the main queue's backlog, the in-flight send included.
	Pending int `json:"pending"`
	// NewContactsHeld is how many first-contact sends are still waiting in
	// the new-contact stage, before they reach the main queue at all.
	NewContactsHeld int `json:"new_contacts_held"`
}

func (q *sendQueue) status() sendQueueStatus {
	q.mu.Lock()
	defer q.mu.Unlock()
	return sendQueueStatus{Pending: q.depth, NewContactsHeld: q.held}
}

func newSendID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("snd-%d", time.Now().UnixNano())
	}
	return "snd-" + hex.EncodeToString(b[:])
}
