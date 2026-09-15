package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// scheduledQueue is the one piece of machinery behind both the main send
// queue and the new-contact stage: a mutex-protected, due-time-ordered list
// of jobs, drained by a single worker that waits for each job's turn and then
// hands it to onDue.
//
// The due time a job gets — max(now, the queue's last-assigned due time) plus
// one gate draw — is decided once, when the job joins the queue, not redrawn
// when the worker finally gets to it. That single change is what makes a
// restart simple: a queued row's due_at on disk already says exactly when it
// is still owed to leave, so recovering the queue is just reloading rows in
// due order and letting the worker pick up where the gate was, via lastDue
// (itself recovered from the newest due_at any row of this stage was ever
// given, queued or finished — see recoverLastDue).
//
// The main queue and the new-contact stage are each one scheduledQueue,
// differing only in their gate, their depth limit, and onDue: for the main
// queue, onDue actually sends; for the new-contact stage (see newcontacts.go)
// it is a thin layer that waits out the reach-out time-lock if needed and
// then hands the job on to the main queue's own schedule.
type scheduledQueue struct {
	stage      string // persisted in each row's `stage` column: stageMain or stageNewContact
	gate       *sendGate
	depthLimit int
	store      *sendQueueStore // nil: no persistence

	onDue      func(ctx context.Context, job *sendJob)
	onShutdown func(job *sendJob)
	// onScheduled runs synchronously inside schedule/insertShared, before the
	// job becomes visible to the worker (i.e. before q.mu is released). That
	// ordering is what it is for: sendQueue uses it to record a job's
	// "queued" booking before any worker goroutine could possibly race ahead
	// and record its outcome first.
	onScheduled func(job *sendJob, queuedAt time.Time)

	mu       sync.Mutex
	jobs     []*scheduledJob
	lastDue  time.Time
	inFlight bool
	wake     chan struct{}
}

// scheduledJob is one entry in a scheduledQueue.
type scheduledJob struct {
	job   *sendJob
	dueAt time.Time
}

const (
	stageMain       = "main"
	stageNewContact = "new_contact"
)

func newScheduledQueue(stage string, gate *sendGate, depthLimit int, onDue func(context.Context, *sendJob)) *scheduledQueue {
	return &scheduledQueue{
		stage:      stage,
		gate:       gate,
		depthLimit: depthLimit,
		onDue:      onDue,
		wake:       make(chan struct{}, 1),
	}
}

// depth is how many jobs this queue currently holds, the one being handed to
// onDue included.
func (q *scheduledQueue) depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := len(q.jobs)
	if q.inFlight {
		n++
	}
	return n
}

// backlogEstimate is a rough "how long to clear what is already here",
// deterministic rather than a real draw — used to estimate the wait a job
// will face once it reaches a queue whose actual due times are not decided
// yet (the main queue, from the point of view of a first contact still held
// in the new-contact stage).
func (q *scheduledQueue) backlogEstimate() time.Duration {
	return time.Duration(q.depth()) * q.gate.mean
}

// nextDueLocked computes the due time a brand-new arrival gets: the last
// due time this queue handed out, plus one gate draw, or now if that is
// still in the past (an idle queue is due immediately, since its lastDue is
// far behind). lastDue is then set to exactly this job's due time, which is
// what makes recovering it after a restart trivial — it is simply the
// newest due_at any row of this queue was ever given (see recoverLastDue).
// Must be called with q.mu held.
func (q *scheduledQueue) nextDueLocked(now time.Time) time.Time {
	if !q.gate.enabled() {
		return now
	}
	due := q.lastDue.Add(q.gate.draw())
	if due.Before(now) {
		due = now
	}
	q.lastDue = due
	return due
}

// schedule adds a brand-new job at a freshly computed due time. ok is false
// once depthLimit jobs are already held, in which case nothing changes and
// the caller still owns cleanup.
func (q *scheduledQueue) schedule(job *sendJob, queuedAt time.Time) (dueAt time.Time, ahead int, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) >= q.depthLimit {
		return time.Time{}, 0, false
	}
	ahead = len(q.jobs)
	dueAt = q.nextDueLocked(time.Now())
	if q.onScheduled != nil {
		q.onScheduled(job, queuedAt)
	}
	q.insertLocked(job, dueAt)
	return dueAt, ahead, true
}

// insertShared adds job at a due time it inherits from another job already in
// this queue — a new-contact follow-up sharing its first message's slot — so
// it spends no gap of its own and does not advance lastDue. ok is false once
// depthLimit jobs are already held.
func (q *scheduledQueue) insertShared(job *sendJob, dueAt, queuedAt time.Time) (ahead int, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) >= q.depthLimit {
		return 0, false
	}
	ahead = len(q.jobs)
	if q.onScheduled != nil {
		q.onScheduled(job, queuedAt)
	}
	q.insertLocked(job, dueAt)
	return ahead, true
}

// relocate moves a job that is already on disk into this queue at a freshly
// computed due time, persisting the row's new stage and due time — used when
// the new-contact stage hands a released job on to the main queue. ok is
// false once depthLimit jobs are already held.
func (q *scheduledQueue) relocate(job *sendJob) (dueAt time.Time, ok bool) {
	q.mu.Lock()
	if len(q.jobs) >= q.depthLimit {
		q.mu.Unlock()
		return time.Time{}, false
	}
	dueAt = q.nextDueLocked(time.Now())
	q.insertLocked(job, dueAt)
	q.mu.Unlock()
	q.persistSchedule(job.id, dueAt)
	return dueAt, true
}

// insertLocked inserts job in due-ascending order, stable for ties (a
// follow-up sharing a due time lands after whatever is already there at that
// same instant), and wakes the worker in case it is idle or waiting on a
// later job. Must be called with q.mu held.
func (q *scheduledQueue) insertLocked(job *sendJob, dueAt time.Time) {
	i := len(q.jobs)
	for i > 0 && q.jobs[i-1].dueAt.After(dueAt) {
		i--
	}
	q.jobs = append(q.jobs, nil)
	copy(q.jobs[i+1:], q.jobs[i:])
	q.jobs[i] = &scheduledJob{job: job, dueAt: dueAt}
	q.wakeLocked()
}

func (q *scheduledQueue) wakeLocked() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// remove drops a job that never actually made it (its row failed to persist
// after schedule/insertShared already placed it here). It is a no-op if the
// job has already been picked up by the worker.
func (q *scheduledQueue) remove(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, e := range q.jobs {
		if e.job.id == id {
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
			return
		}
	}
}

// peek is the head of the queue, without removing it.
func (q *scheduledQueue) peek() (*scheduledJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return nil, false
	}
	return q.jobs[0], true
}

// popFront removes and returns the head, if there still is one.
func (q *scheduledQueue) popFront() (*scheduledJob, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return nil, false
	}
	e := q.jobs[0]
	q.jobs = q.jobs[1:]
	return e, true
}

// shiftFrom pushes every job at or after index later by delta, preserving
// their relative spacing, and persists each new due time. Used when a job's
// actual departure is delayed past its due time (the new-contact stage
// waiting out a reach-out time-lock): without this, everyone held behind it
// would leave in a bunch the moment the delay ends.
func (q *scheduledQueue) shiftFrom(index int, delta time.Duration) {
	if delta <= 0 {
		return
	}
	q.mu.Lock()
	type shifted struct {
		id  string
		due time.Time
	}
	var moved []shifted
	for i := index; i < len(q.jobs); i++ {
		q.jobs[i].dueAt = q.jobs[i].dueAt.Add(delta)
		moved = append(moved, shifted{id: q.jobs[i].job.id, due: q.jobs[i].dueAt})
	}
	q.lastDue = q.lastDue.Add(delta)
	q.mu.Unlock()
	for _, m := range moved {
		q.persistSchedule(m.id, m.due)
	}
}

func (q *scheduledQueue) persistSchedule(id string, dueAt time.Time) {
	if q.store == nil {
		return
	}
	if err := q.store.setSchedule(id, q.stage, dueAt); err != nil {
		fmt.Printf("Send queue (%s): failed to persist %s's schedule: %v\n", q.stage, id, err)
	}
}

// recoverLastDue seeds lastDue from the newest due time any row of this
// stage was ever given, queued or already finished, so a fresh process picks
// up spacing exactly where the last one left off — even if the stage is
// empty right after a restart (nothing queued, but the last first contact
// went out ten minutes ago and the next one still owes the rest of its gap).
func (q *scheduledQueue) recoverLastDue(max time.Time) {
	if max.IsZero() {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if max.After(q.lastDue) {
		q.lastDue = max
	}
}

// load seeds the queue with rows recovered from disk. Callers must pass them
// pre-sorted by due time (recovery loads them that way already) and must do
// so before run starts — load does not wake a worker that might already be
// running concurrently.
func (q *scheduledQueue) load(entries []*scheduledJob) {
	if len(entries) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, entries...)
}

// run drives the queue until ctx is cancelled: peek the head, wait for its
// due time (re-checking if something changes — a new arrival due sooner, or
// a shift), then pop it and hand it to onDue. One goroutine per queue, so
// onDue can take its time without another job's turn arriving early.
func (q *scheduledQueue) run(ctx context.Context) {
	for {
		e, ok := q.peek()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-q.wake:
			}
			continue
		}
		if wait := time.Until(e.dueAt); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				q.drainAll()
				return
			case <-q.wake:
				t.Stop()
				continue
			case <-t.C:
			}
		}
		job, ok := q.popFront()
		if !ok {
			continue // raced with a shutdown drain or similar; re-peek
		}
		if ctx.Err() != nil {
			q.onShutdown(job.job)
			q.drainAll()
			return
		}
		q.mu.Lock()
		q.inFlight = true
		q.mu.Unlock()
		q.onDue(ctx, job.job)
		q.mu.Lock()
		q.inFlight = false
		q.mu.Unlock()
	}
}

// drainAll answers every job still held when the bridge stops, without
// running onDue (nothing has been sent) and without touching its persisted
// row — it stays "queued" on disk exactly as submitted, so the next start's
// recovery picks it back up.
func (q *scheduledQueue) drainAll() {
	kept := 0
	for {
		e, ok := q.popFront()
		if !ok {
			break
		}
		q.onShutdown(e.job)
		kept++
	}
	if kept > 0 {
		fmt.Printf("Send queue (%s): stopping with %d submission(s) kept queued on disk to resume after the next start\n", q.stage, kept)
	}
}
