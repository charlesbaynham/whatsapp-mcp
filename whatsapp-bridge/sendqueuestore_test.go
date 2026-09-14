package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// persistentTestQueue builds a queue backed by a real, temp-dir sqlite store
// — never started (no run goroutine) — so a test can submit jobs, inspect
// what got persisted, and simulate "the bridge stops before anything sends"
// just by never running it.
func persistentTestQueue(t *testing.T, storeDir string, gate, ncGate *sendGate, known []string) *sendQueue {
	t.Helper()
	q := newSendQueue(gate, func(context.Context, SendMessageRequest) (bool, string) { return true, "sent" })
	if ncGate != nil {
		chats := fakeChats{known: make(map[string]bool)}
		for _, jid := range known {
			chats.known[jid] = true
		}
		q.newContacts = newNewContactStage(ncGate, func(recipient string) bool {
			return isNewContact(chats, recipient)
		}, func() (bool, time.Time) { return false, time.Time{} })
	}
	store, err := openSendQueueStore(storeDir)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	if _, err := q.attachStore(store); err != nil {
		t.Fatalf("attachStore: %v", err)
	}
	return q
}

// reopen simulates a restart: a brand-new queue over the same directory.
func reopen(t *testing.T, storeDir string, gate, ncGate *sendGate, known []string) (*sendQueue, int) {
	t.Helper()
	q := newSendQueue(gate, func(context.Context, SendMessageRequest) (bool, string) { return true, "sent" })
	if ncGate != nil {
		chats := fakeChats{known: make(map[string]bool)}
		for _, jid := range known {
			chats.known[jid] = true
		}
		q.newContacts = newNewContactStage(ncGate, func(recipient string) bool {
			return isNewContact(chats, recipient)
		}, func() (bool, time.Time) { return false, time.Time{} })
	}
	store, err := openSendQueueStore(storeDir)
	if err != nil {
		t.Fatalf("reopening store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	recovered, err := q.attachStore(store)
	if err != nil {
		t.Fatalf("attachStore on reopen: %v", err)
	}
	return q, recovered
}

func TestRecoveryRestoresQueuedSubmissionsInOrder(t *testing.T) {
	dir := t.TempDir()
	q := persistentTestQueue(t, dir, newTestGate(time.Hour), newTestGate(time.Hour), []string{"447700900099@s.whatsapp.net"})

	established, _, ok := q.submit(SendMessageRequest{Recipient: "447700900099", Message: "hi"}, func() {})
	if !ok {
		t.Fatal("established submit refused")
	}
	poll, _, ok := q.submit(SendMessageRequest{Recipient: "447700900099", poll: &PollSpec{Question: "q", Options: []string{"a", "b"}, SelectableCount: 1}}, func() {})
	if !ok {
		t.Fatal("poll submit refused")
	}
	upDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(upDir, 0o755); err != nil {
		t.Fatal(err)
	}
	uploadPath := filepath.Join(upDir, "upload-abc.jpg")
	if err := os.WriteFile(uploadPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	upload, _, ok := q.submit(SendMessageRequest{Recipient: "447700900099", MediaPath: uploadPath, uploadName: "photo.jpg"}, func() {})
	if !ok {
		t.Fatal("upload submit refused")
	}
	newContact, pos, ok := q.submit(SendMessageRequest{Recipient: "447700900001"}, func() {})
	if !ok || !pos.newContact {
		t.Fatal("new-contact submit refused or not classified as new")
	}

	// The bridge "stops" here: nothing was ever run, so nothing sent.
	q2, recovered := reopen(t, dir, newTestGate(time.Hour), newTestGate(time.Hour), []string{"447700900099@s.whatsapp.net"})
	if recovered != 4 {
		t.Fatalf("recovered = %d, want 4", recovered)
	}

	mainOrder := []string{}
	for _, e := range q2.main.jobs {
		mainOrder = append(mainOrder, e.job.id)
	}
	wantMain := []string{established.id, poll.id, upload.id}
	for i, id := range wantMain {
		if mainOrder[i] != id {
			t.Fatalf("main queue order = %v, want %v", mainOrder, wantMain)
		}
	}
	if len(q2.newContacts.stage.jobs) != 1 || q2.newContacts.stage.jobs[0].job.id != newContact.id {
		t.Fatalf("new-contact stage did not recover %s", newContact.id)
	}

	// A recovered poll job keeps its spec, and an upload job keeps a cleanup
	// that will remove its file.
	var recoveredPoll, recoveredUpload *sendJob
	for _, e := range q2.main.jobs {
		if e.job.id == poll.id {
			recoveredPoll = e.job
		}
		if e.job.id == upload.id {
			recoveredUpload = e.job
		}
	}
	if recoveredPoll == nil || recoveredPoll.req.poll == nil || recoveredPoll.req.poll.Question != "q" {
		t.Fatalf("recovered poll job = %+v", recoveredPoll)
	}
	if recoveredUpload == nil {
		t.Fatal("recovered upload job missing")
	}
	recoveredUpload.cleanup()
	if _, err := os.Stat(uploadPath); !os.IsNotExist(err) {
		t.Fatal("recovered upload's cleanup did not remove its file")
	}

	// Results for all four must read back as queued.
	for _, id := range []string{established.id, poll.id, upload.id, newContact.id} {
		res, ok := q2.result(id)
		if !ok || res.State != sendQueued {
			t.Fatalf("result(%s) = %+v, ok=%v, want queued", id, res, ok)
		}
	}
}

func TestFinishedRowsAreNotRecovered(t *testing.T) {
	dir := t.TempDir()
	q := persistentTestQueue(t, dir, newTestGate(0), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go q.run(ctx)
	job, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
	if !ok {
		t.Fatal("submit refused")
	}
	waitFor(t, job.done)
	cancel()

	q2, recovered := reopen(t, dir, newTestGate(0), nil, nil)
	if recovered != 0 {
		t.Fatalf("recovered %d finished job(s), want 0", recovered)
	}
	res, ok := q2.result(job.id)
	if !ok || res.State != sendSent || !res.Success {
		t.Fatalf("result after restart = %+v, ok=%v, want the original sent outcome", res, ok)
	}
}

func TestOrphanUploadSweptReferencedKept(t *testing.T) {
	dir := t.TempDir()
	// Construct (and so recover/sweep) against an empty store first — with
	// nothing on disk yet, there is nothing for that first sweep to find.
	q := persistentTestQueue(t, dir, newTestGate(time.Hour), nil, nil)

	tmpDir := filepath.Join(dir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	referencedPath := filepath.Join(tmpDir, "upload-keep.jpg")
	orphanPath := filepath.Join(tmpDir, "upload-orphan.jpg")
	for _, p := range []string{referencedPath, orphanPath} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Only referencedPath is ever attached to a submission — orphanPath
	// simulates a crash between writing an upload and persisting its row.
	if _, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000", MediaPath: referencedPath, uploadName: "keep.jpg"}, func() {}); !ok {
		t.Fatal("submit refused")
	}

	// The restart's recovery is what sweeps orphans.
	reopen(t, dir, newTestGate(time.Hour), nil, nil)

	if _, err := os.Stat(referencedPath); err != nil {
		t.Fatalf("a referenced (still-queued) upload was swept: %v", err)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatal("an orphaned upload was not swept")
	}
}

func TestDBOpenFailureFallsBackToMemoryOnly(t *testing.T) {
	dir := t.TempDir()
	// A directory where the db file should go, so sql.Open's file can never
	// be created.
	blocked := filepath.Join(dir, "sendqueue.db")
	if err := os.MkdirAll(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := openSendQueueStore(dir); err == nil {
		t.Fatal("expected opening the store to fail when its path is a directory")
	}

	// The queue itself must still work perfectly well without persistence.
	q := newTestQueue(t, func(context.Context, SendMessageRequest) (bool, string) { return true, "sent" })
	job, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
	if !ok {
		t.Fatal("memory-only queue refused a submission")
	}
	res := waitFor(t, job.done)
	if !res.Success {
		t.Fatalf("memory-only send failed: %+v", res)
	}
}

func TestPersistFailureRefusesTheSubmission(t *testing.T) {
	dir := t.TempDir()
	store, err := openSendQueueStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Close the underlying db so the next write fails, without the queue
	// knowing anything happened to it.
	store.Close()

	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) {
		t.Fatal("a submission that failed to persist must never reach the send function")
		return false, ""
	})
	q.store = store
	q.main.store = store

	before := q.pending()
	_, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
	if ok {
		t.Fatal("submit succeeded despite the store being unusable")
	}
	if got := q.pending(); got != before {
		t.Fatalf("pending = %d after a refused submission, want unchanged %d", got, before)
	}
}

func TestDrainLeavesRowsQueuedOnDisk(t *testing.T) {
	dir := t.TempDir()
	q := persistentTestQueue(t, dir, newTestGate(time.Hour), nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go q.run(ctx)

	first, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
	if !ok {
		t.Fatal("first submit refused")
	}
	held, _, ok := q.submit(SendMessageRequest{Recipient: "447700900001"}, func() {})
	if !ok {
		t.Fatal("second submit refused")
	}
	waitFor(t, first.done) // due immediately; held stays behind the hour-long gate
	cancel()

	res := waitFor(t, held.done)
	if res.State != sendQueued {
		t.Fatalf("result = %+v, want still queued", res)
	}

	store2, err := openSendQueueStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	loaded, err := store2.loadQueued(stageMain)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].ID != held.id {
		t.Fatalf("queued rows on disk = %+v, want just %s", loaded, held.id)
	}
}

// A held new contact whose due time has not arrived yet must still wait for
// it after a restart — recovery does not let it jump the queue just because
// the process restarted while it was waiting.
func TestRestartedNewContactStillWaitsForItsDueTime(t *testing.T) {
	dir := t.TempDir()
	q := persistentTestQueue(t, dir, newTestGate(0), newTestGate(300*time.Millisecond), nil)

	// Prime the stage so it is not idle: the first contact is due
	// immediately (nothing to space it from yet); the second is genuinely
	// due a gate gap later (at least the ~1s floor newTestGate uses).
	if _, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {}); !ok {
		t.Fatal("first submit refused")
	}
	submitted := time.Now()
	job, _, ok := q.submit(SendMessageRequest{Recipient: "447700900001"}, func() {})
	if !ok {
		t.Fatal("second submit refused")
	}
	var dueAt time.Time
	for _, e := range q.newContacts.stage.jobs {
		if e.job.id == job.id {
			dueAt = e.dueAt
		}
	}
	if wait := dueAt.Sub(submitted); wait < 500*time.Millisecond {
		t.Fatalf("second job's due time is only %s out; the test needs it meaningfully in the future", wait)
	}

	// "Restart": nothing was ever run, so nothing sent.
	q2, recovered := reopen(t, dir, newTestGate(0), newTestGate(300*time.Millisecond), nil)
	if recovered != 2 {
		t.Fatalf("recovered = %d, want 2", recovered)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q2.run(ctx)

	// It must not fire before its recovered due time...
	if wait := time.Until(dueAt) - 100*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	if time.Now().Before(dueAt) {
		if res, ok := q2.result(job.id); ok && res.State == sendSent {
			t.Fatal("the recovered new contact fired before its due time")
		}
	}

	// ...but must fire once it arrives.
	deadline := time.After(3 * time.Second)
	for {
		res, _ := q2.result(job.id)
		if res.State == sendSent {
			break
		}
		select {
		case <-deadline:
			res, _ := q2.result(job.id)
			t.Fatalf("the recovered new contact never fired: %+v", res)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// A fresh new-contact submission right after a restart must still be spaced
// from the last one that actually went out before the restart — recovering
// lastDue from a *finished* row's preserved new_contact_due_at, not just
// from whatever is still queued (there may be nothing queued at all).
func TestFreshSubmissionAfterRestartSpacedFromLastFinishedNewContact(t *testing.T) {
	dir := t.TempDir()
	gateMean := 300 * time.Millisecond
	q := persistentTestQueue(t, dir, newTestGate(0), newTestGate(gateMean), nil)
	ctx, cancel := context.WithCancel(context.Background())
	first, _, ok := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
	if !ok {
		t.Fatal("submit refused")
	}
	go q.run(ctx)
	res := waitFor(t, first.done)
	if !res.Success {
		t.Fatalf("first new contact failed: %+v", res)
	}
	// Read the exact due time the (now finished, and relocated to the main
	// queue) first job originally got in the new-contact stage: its row's
	// due_at was overwritten on release, but new_contact_due_at was not.
	firstDueAt, err := q.store.maxNewContactDueAt()
	if err != nil {
		t.Fatal(err)
	}
	if firstDueAt.IsZero() {
		t.Fatal("first job's new-contact due time was not preserved")
	}
	cancel()

	// Restart with nothing left queued at all.
	q2, recovered := reopen(t, dir, newTestGate(0), newTestGate(gateMean), nil)
	if recovered != 0 {
		t.Fatalf("recovered = %d, want 0 (nothing was left queued)", recovered)
	}

	submitTime := time.Now()
	second, pos, ok := q2.submit(SendMessageRequest{Recipient: "447700900001"}, func() {})
	if !ok || !pos.newContact {
		t.Fatal("second submit refused or not a new contact")
	}
	var dueAt time.Time
	for _, e := range q2.newContacts.stage.jobs {
		if e.job.id == second.id {
			dueAt = e.dueAt
		}
	}
	if dueAt.IsZero() {
		t.Fatal("second job not found in the new-contact stage")
	}
	// The discriminating check: if lastDue had not been recovered, the fresh
	// stage would think it was idle and this due time would be immediate
	// (~submitTime). Recovering it from firstDueAt instead means this job
	// must wait out at least the gate's floor from there — which, since the
	// whole test runs in well under a second, is necessarily later than
	// "now" by a non-trivial amount.
	if wait := dueAt.Sub(submitTime); wait < 500*time.Millisecond {
		t.Fatalf("second due time was only %s after submission — lastDue was not recovered from the first session", wait)
	}
	if dueAt.Before(firstDueAt) {
		t.Fatalf("second due (%s) is before the first session's last due time (%s)", dueAt, firstDueAt)
	}
}

func TestQueuedMessageMentionsRestartWording(t *testing.T) {
	// Sanity check that the wording used elsewhere in these tests
	// ("restart") actually appears in the real still-queued message, so the
	// substring checks above are not accidentally testing nothing.
	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) { return true, "sent" })
	job := &sendJob{id: "x", done: make(chan SendResult, 1)}
	q.stillQueued(job)
	res := <-job.done
	if !strings.Contains(res.Message, "restart") {
		t.Fatalf("stillQueued message = %q, wants to mention a restart", res.Message)
	}
}

func TestReleasedButUnsentFirstContactStaysReleasedAfterRestart(t *testing.T) {
	dir := t.TempDir()
	q := persistentTestQueue(t, dir, newTestGate(time.Hour), newTestGate(time.Hour), nil)

	// The stage released this first contact onto the main queue (its row is
	// stage=main, new_contact=1) and the bridge stopped before it was sent.
	job := &sendJob{id: "snd-released", req: SendMessageRequest{Recipient: "447700900000", Message: "hi"}}
	if err := q.store.insertJob(job, true, stageMain, time.Now().Add(time.Minute), time.Now()); err != nil {
		t.Fatal(err)
	}

	q2, recovered := reopen(t, dir, newTestGate(time.Hour), newTestGate(time.Hour), nil)
	if recovered != 1 || q2.main.depth() != 1 {
		t.Fatalf("recovered %d, main depth %d; want the released send back on the main queue", recovered, q2.main.depth())
	}
	if q2.newContacts.holds("447700900000") {
		t.Fatal("a first contact already released before the restart is being held again")
	}
	_, pos, ok := q2.submit(SendMessageRequest{Recipient: "447700900000", Message: "follow-up"}, func() {})
	if !ok || pos.newContact {
		t.Fatalf("follow-up position = %+v ok=%v; want it straight onto the main queue behind the released send", pos, ok)
	}
	if q2.main.depth() != 2 || q2.newContacts.stage.depth() != 0 {
		t.Fatalf("main depth %d, stage depth %d; want both sends on the main queue", q2.main.depth(), q2.newContacts.stage.depth())
	}
}
