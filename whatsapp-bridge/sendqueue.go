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

// sendQueue serialises outbound sends through the main scheduledQueue,
// spacing them with its gate. Submissions are accepted, persisted and
// acknowledged immediately; a caller that wants the outcome waits on the
// returned channel instead.
//
// A send to someone the account has never messaged does not join the main
// queue directly: it waits in newContacts first (see newcontacts.go), and is
// handed on to the main queue — subject to the ordinary gate like everything
// else — only when that stage releases it.
//
// Every submission and outcome is written to store (nil disables
// persistence, falling back to memory-only behaviour) before it is acted on,
// so a restart can pick the queue back up where it left off; see
// attachStore.
type sendQueue struct {
	main        *scheduledQueue
	newContacts *newContactStage // nil: first contacts are not held separately
	send        func(ctx context.Context, req SendMessageRequest) (bool, string)
	store       *sendQueueStore

	wireOnce sync.Once

	mu      sync.Mutex
	results map[string]SendResult
	order   []string
}

func newSendQueue(gate *sendGate, send func(context.Context, SendMessageRequest) (bool, string)) *sendQueue {
	q := &sendQueue{
		send:    send,
		results: make(map[string]SendResult, resultsKept),
	}
	q.main = newScheduledQueue(stageMain, gate, queueDepthFromEnv(), q.mainOnDue)
	q.main.onShutdown = q.stillQueued
	q.main.onScheduled = func(job *sendJob, queuedAt time.Time) {
		q.record(SendResult{ID: job.id, State: sendQueued, QueuedAt: queuedAt})
	}
	return q
}

// queueDepthFromEnv is how many submissions a queue accepts before refusing
// more. A backlog deeper than the results kept would evict a record while its
// message is still waiting, so /api/send/{id} would 404 on a live send; the
// main queue and the new-contact stage each hold this many, so they share
// the results budget.
func queueDepthFromEnv() int {
	return min(envPositiveInt("WHATSAPP_SEND_MAX_QUEUE_DEPTH", defaultQueueDepth), resultsKept/2)
}

// ensureWired connects the new-contact stage's callbacks to this queue. It
// has to happen after newContacts is assigned (a plain field, set by the
// caller after construction) but must not race a submission that arrives
// just as run is starting, so both run and submit call it and sync.Once
// makes whichever gets there first the one that does it.
func (q *sendQueue) ensureWired() {
	q.wireOnce.Do(func() {
		if q.newContacts == nil {
			return
		}
		nc := q.newContacts
		nc.stage.onDue = nc.makeOnDue(q.main, q.stillQueued)
		nc.stage.onShutdown = q.stillQueued
		nc.stage.onScheduled = func(job *sendJob, queuedAt time.Time) {
			q.record(SendResult{ID: job.id, State: sendQueued, NewContact: true, QueuedAt: queuedAt})
		}
	})
}

// run drives the queue until ctx is cancelled: one worker for the main queue
// and, if first contacts are held separately, one for the new-contact stage,
// so sends never overlap and each stage's gate is advanced from a single
// place.
func (q *sendQueue) run(ctx context.Context) {
	q.ensureWired()
	if q.newContacts != nil {
		go q.newContacts.stage.run(ctx)
	}
	q.main.run(ctx)
}

// mainOnDue is the main queue's onDue: actually send, then record the
// outcome. Cleanup only runs once a send is genuinely attempted — not on a
// job kept queued for a restart, whose upload (if any) must still be there
// next time.
func (q *sendQueue) mainOnDue(ctx context.Context, job *sendJob) {
	if ctx.Err() != nil {
		q.stillQueued(job)
		return
	}
	defer job.cleanup()
	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	success, message := q.send(sendCtx, job.req)
	fmt.Println("Message sent", success, message)
	q.finish(job, success, message)
}

// queuePosition is where a submission landed and what that implies for the
// caller: how many sends are ahead of it (on the main queue, or in the
// new-contact stage when newContact is set, where ahead counts the other
// first contacts still waiting there) and a rough expected wait before it
// goes out.
type queuePosition struct {
	ahead      int
	newContact bool
	wait       time.Duration
}

// submit persists and queues a job, returning it and its position. ok is
// false when the queue is full or the row could not be persisted, in which
// case nothing is queued and the caller still owns cleanup.
func (q *sendQueue) submit(req SendMessageRequest, cleanup func()) (job *sendJob, pos queuePosition, ok bool) {
	q.ensureWired()
	job = &sendJob{id: newSendID(), req: req, cleanup: cleanup, done: make(chan SendResult, 1)}
	pos.newContact = q.newContacts != nil && q.newContacts.holds(req.Recipient)
	queuedAt := time.Now()

	var dueAt time.Time
	var established bool
	stage := stageMain
	if pos.newContact {
		stage = stageNewContact
		dueAt, pos.ahead, ok, established = q.newContacts.submit(job, queuedAt)
	} else {
		dueAt, pos.ahead, ok = q.main.schedule(job, queuedAt)
	}
	if !ok {
		return nil, queuePosition{}, false
	}
	pos.wait = q.estimateWait(pos.newContact, dueAt)

	if q.store != nil {
		if err := q.store.insertJob(job, pos.newContact, stage, dueAt, queuedAt); err != nil {
			fmt.Printf("Send queue: failed to persist submission %s, refusing it: %v\n", job.id, err)
			if pos.newContact {
				q.newContacts.unsubmit(job, established)
			} else {
				q.main.remove(job.id)
			}
			q.forget(job.id)
			return nil, queuePosition{}, false
		}
	}
	return job, pos, true
}

// estimateWait is what queuedMessage tells the caller to expect: the due
// time already committed for a plain send; for a new contact, that plus a
// rough allowance for the main queue's current backlog, since the main-queue
// due time it will actually get is not decided until the stage releases it.
func (q *sendQueue) estimateWait(newContact bool, dueAt time.Time) time.Duration {
	wait := waitFrom(dueAt)
	if newContact {
		wait += q.main.backlogEstimate()
	}
	return wait
}

// waitFrom is how far off an instant is, or zero once it has passed.
func waitFrom(t time.Time) time.Duration {
	if d := time.Until(t); d > 0 {
		return d
	}
	return 0
}

// stillQueued answers a job's caller when the bridge is stopping before the
// job could be sent. Unlike finish, it leaves the job's persisted row (and
// its upload, if any) alone: the row stays "queued" on disk, so the next
// start's recovery picks the job back up exactly where it was, still owed
// whatever spacing it was waiting on.
func (q *sendQueue) stillQueued(job *sendJob) {
	booked := q.booking(job.id)
	res := SendResult{
		ID: job.id, State: sendQueued, Success: false,
		Message:    "the bridge is restarting; this send is still queued and will resume once it is back",
		NewContact: booked.NewContact, QueuedAt: booked.QueuedAt,
	}
	job.done <- res
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
	if q.store != nil {
		if err := q.store.finishJob(job.id, success, message, res.SentAt); err != nil {
			fmt.Printf("Send queue: failed to persist the outcome of %s: %v\n", job.id, err)
		} else if err := q.store.prune(); err != nil {
			fmt.Printf("Send queue: failed to prune old sends: %v\n", err)
		}
	}
	q.record(res)
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

// forget drops a booking for a submission that was scheduled but whose row
// then failed to persist, so it was never actually queued.
func (q *sendQueue) forget(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.results, id)
	for i, known := range q.order {
		if known == id {
			q.order = append(q.order[:i], q.order[i+1:]...)
			break
		}
	}
}

// result looks a submission up first in memory, then — if this process never
// saw it, because it finished before the last restart — in the durable
// store, so GET /api/send/{id} keeps working for a submission from before
// the bridge came back.
func (q *sendQueue) result(id string) (SendResult, bool) {
	q.mu.Lock()
	res, ok := q.results[id]
	q.mu.Unlock()
	if ok {
		return res, true
	}
	if q.store == nil {
		return SendResult{}, false
	}
	res, ok, err := q.store.getResult(id)
	if err != nil {
		fmt.Printf("Send queue: failed to read %s from the durable store: %v\n", id, err)
		return SendResult{}, false
	}
	return res, ok
}

// pending is how many submissions have not yet finished, wherever they are.
func (q *sendQueue) pending() int {
	n := q.main.depth()
	if q.newContacts != nil {
		n += q.newContacts.stage.depth()
	}
	return n
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
	held := 0
	if q.newContacts != nil {
		held = q.newContacts.stage.depth()
	}
	return sendQueueStatus{Pending: q.main.depth(), NewContactsHeld: held}
}

// attachStore wires a durable store into an already-constructed queue: each
// stage's scheduling state is restored, every submission still queued from
// before the restart is recovered onto the right stage in its original
// order, and orphaned uploads are swept. Only once all of that has succeeded
// does the queue start persisting further submissions and outcomes — a
// failure here leaves the queue exactly as it was, memory-only, rather than
// half-wired to a store it could not fully read. Call it once, after
// newContacts is set (if it is) and before run.
func (q *sendQueue) attachStore(store *sendQueueStore) (recovered int, err error) {
	mainMax, err := store.maxDueAt(stageMain)
	if err != nil {
		return 0, fmt.Errorf("reading the main queue's last due time: %v", err)
	}
	mainRows, err := store.loadQueued(stageMain)
	if err != nil {
		return 0, fmt.Errorf("loading queued submissions: %v", err)
	}
	var ncMax time.Time
	var ncRows []persistedRow
	if q.newContacts != nil {
		ncMax, err = store.maxNewContactDueAt()
		if err != nil {
			return 0, fmt.Errorf("reading the new-contact stage's last due time: %v", err)
		}
		ncRows, err = store.loadQueued(stageNewContact)
		if err != nil {
			return 0, fmt.Errorf("loading queued new-contact submissions: %v", err)
		}
	}

	// Everything above only reads; only past this point do we start changing
	// state, once we know there is something consistent to build on.
	q.store = store
	q.main.store = store
	q.main.recoverLastDue(mainMax)

	referenced := map[string]bool{}
	for _, r := range mainRows {
		q.loadRecoveredRow(q.main, r, referenced)
	}
	recovered += len(mainRows)

	if q.newContacts != nil {
		q.newContacts.stage.store = store
		q.newContacts.stage.recoverLastDue(ncMax)
		for _, r := range ncRows {
			q.loadRecoveredRow(q.newContacts.stage, r, referenced)
			q.newContacts.trackHeld(r)
		}
		recovered += len(ncRows)
	}

	if removed, sweepErr := sweepOrphanUploads(store.storeDir, referenced); sweepErr != nil {
		fmt.Printf("Send queue: failed to sweep orphaned uploads: %v\n", sweepErr)
	} else if removed > 0 {
		fmt.Printf("Send queue: removed %d orphaned upload(s) left from before the last restart\n", removed)
	}
	return recovered, nil
}

// loadRecoveredRow reconstructs a job from a persisted row, records its
// still-queued booking and places it on dest at its original due time.
func (q *sendQueue) loadRecoveredRow(dest *scheduledQueue, r persistedRow, referenced map[string]bool) {
	if r.MediaPath != "" {
		referenced[r.MediaPath] = true
	}
	job := &sendJob{id: r.ID, req: r.request(), cleanup: uploadCleanup(r), done: make(chan SendResult, 1)}
	dest.load([]*scheduledJob{{job: job, dueAt: r.DueAt}})
	q.record(SendResult{ID: job.id, State: sendQueued, NewContact: r.NewContact, QueuedAt: r.QueuedAt})
}

func newSendID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("snd-%d", time.Now().UnixNano())
	}
	return "snd-" + hex.EncodeToString(b[:])
}
