package main

import (
	"context"
	"testing"
	"time"
)

// testScheduledQueue builds a scheduledQueue whose onDue just records which
// job it saw, for tests that only care about scheduling and ordering rather
// than what happens once a job is due.
func testScheduledQueue(gate *sendGate, depth int) (*scheduledQueue, chan *sendJob) {
	seen := make(chan *sendJob, depth+10)
	q := newScheduledQueue(stageMain, gate, depth, func(_ context.Context, job *sendJob) {
		seen <- job
	})
	q.onShutdown = func(job *sendJob) { seen <- job }
	return q, seen
}

func testJob(id string) *sendJob {
	return &sendJob{id: id, done: make(chan SendResult, 1), cleanup: func() {}}
}

func TestScheduleIsDueImmediatelyWhenIdle(t *testing.T) {
	q, _ := testScheduledQueue(newTestGate(30*time.Second), 10)
	now := time.Now()
	due, ahead, ok := q.schedule(testJob("a"), now)
	if !ok || ahead != 0 {
		t.Fatalf("ahead = %d, ok = %v, want 0, true", ahead, ok)
	}
	if due.Sub(now) > time.Second {
		t.Fatalf("idle queue held the first job back by %s", due.Sub(now))
	}
}

func TestSuccessiveSchedulesAreSpacedByTheGap(t *testing.T) {
	q, _ := testScheduledQueue(newTestGate(30*time.Second), 200)
	now := time.Now()
	prev, _, _ := q.schedule(testJob("a"), now)
	for i := 0; i < 100; i++ {
		due, ahead, ok := q.schedule(testJob(string(rune('b'+i))), now)
		if !ok {
			t.Fatalf("schedule %d refused", i)
		}
		if ahead != i+1 {
			t.Fatalf("ahead = %d, want %d", ahead, i+1)
		}
		if gap := due.Sub(prev); gap < minGap {
			t.Fatalf("jobs %d and %d are %s apart, below the %s floor", i, i+1, gap, minGap)
		}
		prev = due
	}
}

func TestDisabledGateSchedulesImmediately(t *testing.T) {
	q, _ := testScheduledQueue(newTestGate(0), 10)
	now := time.Now()
	for i := 0; i < 5; i++ {
		due, _, ok := q.schedule(testJob(string(rune('a'+i))), now)
		if !ok || due.Sub(now) > time.Second {
			t.Fatalf("disabled gate held job %d back by %s", i, due.Sub(now))
		}
	}
}

func TestScheduleRefusesPastDepthLimit(t *testing.T) {
	q, _ := testScheduledQueue(newTestGate(time.Hour), 2)
	now := time.Now()
	if _, _, ok := q.schedule(testJob("a"), now); !ok {
		t.Fatal("first schedule refused")
	}
	if _, _, ok := q.schedule(testJob("b"), now); !ok {
		t.Fatal("second schedule refused")
	}
	if _, _, ok := q.schedule(testJob("c"), now); ok {
		t.Fatal("schedule accepted past the depth limit")
	}
	if d := q.depth(); d != 2 {
		t.Fatalf("depth = %d, want 2", d)
	}
}

func TestInsertSharedKeepsOrderAndDoesNotAdvanceLastDue(t *testing.T) {
	q, _ := testScheduledQueue(newTestGate(time.Hour), 10)
	now := time.Now()
	firstDue, _, _ := q.schedule(testJob("first"), now)

	// A follow-up shares the first message's due time and must not push a
	// later brand-new arrival's due time out any further than one gap from
	// the first message.
	ahead, ok := q.insertShared(testJob("followup"), firstDue, now)
	if !ok || ahead != 1 {
		t.Fatalf("insertShared: ahead = %d, ok = %v, want 1, true", ahead, ok)
	}

	otherDue, ahead, ok := q.schedule(testJob("other"), now)
	if !ok || ahead != 2 {
		t.Fatalf("schedule after insertShared: ahead = %d, ok = %v, want 2, true", ahead, ok)
	}
	if otherDue.Before(firstDue) {
		t.Fatalf("a fresh schedule (%s) landed before the shared due time (%s)", otherDue, firstDue)
	}

	// Order in the queue must be [first, followup, other].
	order := []string{}
	for _, e := range q.jobs {
		order = append(order, e.job.id)
	}
	want := []string{"first", "followup", "other"}
	for i, id := range want {
		if order[i] != id {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestShiftFromPreservesRelativeSpacingAndPersists(t *testing.T) {
	dir := t.TempDir()
	store, err := openSendQueueStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	q, _ := testScheduledQueue(newTestGate(time.Hour), 10)
	q.store = store
	now := time.Now()
	dueA, _, _ := q.schedule(testJob("a"), now)
	if err := store.insertJob(testJob("a"), false, stageMain, dueA, now); err != nil {
		t.Fatal(err)
	}
	dueB, _, _ := q.schedule(testJob("b"), now)
	if err := store.insertJob(testJob("b"), false, stageMain, dueB, now); err != nil {
		t.Fatal(err)
	}
	dueC, _, _ := q.schedule(testJob("c"), now)
	if err := store.insertJob(testJob("c"), false, stageMain, dueC, now); err != nil {
		t.Fatal(err)
	}

	// In real use, run pops the head before calling onDue, and onDue is what
	// calls shiftFrom on whatever is left once it discovers a delay — so
	// pop "a" first, then shift what remains.
	if _, ok := q.popFront(); !ok {
		t.Fatal("popFront found nothing")
	}
	delta := 20 * time.Minute
	q.shiftFrom(0, delta)

	if q.jobs[0].job.id != "b" || q.jobs[1].job.id != "c" {
		t.Fatalf("unexpected order after shift: %v", q.jobs)
	}
	if got := q.jobs[0].dueAt; !got.Equal(dueB.Add(delta)) {
		t.Fatalf("b's due = %s, want %s", got, dueB.Add(delta))
	}
	if got := q.jobs[1].dueAt; !got.Equal(dueC.Add(delta)) {
		t.Fatalf("c's due = %s, want %s", got, dueC.Add(delta))
	}
	// Spacing between b and c must be unchanged.
	if got, want := q.jobs[1].dueAt.Sub(q.jobs[0].dueAt), dueC.Sub(dueB); got != want {
		t.Fatalf("spacing after shift = %s, want unchanged %s", got, want)
	}

	rows, err := store.loadQueued(stageMain)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]persistedRow{}
	for _, r := range rows {
		byID[r.ID] = r
	}
	if !byID["b"].DueAt.Equal(dueB.Add(delta)) {
		t.Fatalf("persisted b due = %s, want %s", byID["b"].DueAt, dueB.Add(delta))
	}
	if !byID["c"].DueAt.Equal(dueC.Add(delta)) {
		t.Fatalf("persisted c due = %s, want %s", byID["c"].DueAt, dueC.Add(delta))
	}
	if !byID["a"].DueAt.Equal(dueA) {
		t.Fatalf("a's persisted due time must be untouched by a shift starting at index 1")
	}
}

func TestRecoverLastDueSeedsFutureScheduling(t *testing.T) {
	q, _ := testScheduledQueue(newTestGate(time.Hour), 10)
	future := time.Now().Add(45 * time.Minute)
	q.recoverLastDue(future)

	due, _, ok := q.schedule(testJob("a"), time.Now())
	if !ok {
		t.Fatal("schedule refused")
	}
	if due.Before(future) {
		t.Fatalf("due = %s, want at or after the recovered last-due %s", due, future)
	}

	// A zero time (nothing recovered) must not clobber a real seed.
	q2, _ := testScheduledQueue(newTestGate(time.Hour), 10)
	q2.recoverLastDue(time.Time{})
	due2, _, _ := q2.schedule(testJob("a"), time.Now())
	if due2.After(time.Now().Add(time.Second)) {
		t.Fatalf("an empty recovery must leave an idle queue due immediately, got %s", due2)
	}
}

func TestLoadProcessesInDueOrderAndWaitsForFutureDueTimes(t *testing.T) {
	q, seen := testScheduledQueue(newTestGate(0), 10)
	now := time.Now()
	future := now.Add(150 * time.Millisecond)
	q.load([]*scheduledJob{
		{job: testJob("past"), dueAt: now.Add(-time.Minute)},
		{job: testJob("future"), dueAt: future},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.run(ctx)

	first := <-seen
	if first.id != "past" {
		t.Fatalf("first processed = %s, want the overdue one", first.id)
	}
	select {
	case job := <-seen:
		t.Fatalf("the future job (%s) ran before its due time", job.id)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case job := <-seen:
		if job.id != "future" {
			t.Fatalf("second processed = %s, want future", job.id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the future job never ran once its due time arrived")
	}
}

func TestRunDrainsWithoutInvokingOnDue(t *testing.T) {
	q, seen := testScheduledQueue(newTestGate(0), 10)
	q.load([]*scheduledJob{
		{job: testJob("a"), dueAt: time.Now().Add(time.Hour)},
		{job: testJob("b"), dueAt: time.Now().Add(2 * time.Hour)},
	})
	ctx, cancel := context.WithCancel(context.Background())
	go q.run(ctx)
	time.Sleep(20 * time.Millisecond) // let run start waiting on the head
	cancel()

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case job := <-seen:
			got[job.id] = true
		case <-time.After(2 * time.Second):
			t.Fatal("not every held job was answered on shutdown")
		}
	}
	if !got["a"] || !got["b"] {
		t.Fatalf("answered %v, want both a and b", got)
	}
	if d := q.depth(); d != 0 {
		t.Fatalf("depth after drain = %d, want 0", d)
	}
}
