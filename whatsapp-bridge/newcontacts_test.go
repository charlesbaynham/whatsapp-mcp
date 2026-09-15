package main

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newContactsQueue builds a queue whose main gate never waits and whose
// new-contact stage spaces first contacts by mean. known lists the chats
// the store already holds; everyone else is a first contact.
func newContactsQueue(t *testing.T, mean time.Duration, known []string, lock func() (bool, time.Time), send func(context.Context, SendMessageRequest) (bool, string)) *sendQueue {
	t.Helper()
	q := newSendQueue(newTestGate(0), send)
	chats := fakeChats{known: make(map[string]bool)}
	for _, jid := range known {
		chats.known[jid] = true
	}
	if lock == nil {
		lock = func() (bool, time.Time) { return false, time.Time{} }
	}
	q.newContacts = newNewContactStage(newTestGate(mean), func(recipient string) bool {
		return isNewContact(chats, recipient)
	}, lock)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)
	return q
}

// newContactsQueueUnstarted is newContactsQueue without launching the worker:
// for a test that needs several submissions classified before any of them
// can possibly have already been processed (an idle stage releases its first
// job as soon as a worker goroutine gets scheduled, which can otherwise race
// a second submission a few lines later).
func newContactsQueueUnstarted(t *testing.T, mean time.Duration, known []string, send func(context.Context, SendMessageRequest) (bool, string)) (q *sendQueue, start func()) {
	t.Helper()
	q = newSendQueue(newTestGate(0), send)
	chats := fakeChats{known: make(map[string]bool)}
	for _, jid := range known {
		chats.known[jid] = true
	}
	q.newContacts = newNewContactStage(newTestGate(mean), func(recipient string) bool {
		return isNewContact(chats, recipient)
	}, func() (bool, time.Time) { return false, time.Time{} })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return q, func() { go q.run(ctx) }
}

// recorder collects the recipients a queue's send function is called with.
type recorder struct {
	mu   sync.Mutex
	sent []string
}

func (r *recorder) send(_ context.Context, req SendMessageRequest) (bool, string) {
	r.mu.Lock()
	r.sent = append(r.sent, req.Recipient)
	r.mu.Unlock()
	return true, "sent"
}

func (r *recorder) recipients() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

func mustSubmit(t *testing.T, q *sendQueue, recipient string) (*sendJob, bool) {
	t.Helper()
	job, pos, ok := q.submit(SendMessageRequest{Recipient: recipient}, func() {})
	if !ok {
		t.Fatalf("submit to %s refused", recipient)
	}
	return job, pos.newContact
}

func notSentWithin(t *testing.T, job *sendJob, d time.Duration) {
	t.Helper()
	select {
	case res := <-job.done:
		t.Fatalf("send %s completed (%+v) while it should still be held", job.id, res)
	case <-time.After(d):
	}
}

func TestEstablishedChatSkipsTheNewContactStage(t *testing.T) {
	var rec recorder
	q := newContactsQueue(t, time.Hour, []string{"447700900000@s.whatsapp.net"}, nil, rec.send)
	job, newContact := mustSubmit(t, q, "447700900000")
	if newContact {
		t.Fatal("a recipient with an existing chat was classed as a new contact")
	}
	if res := waitFor(t, job.done); !res.Success || res.NewContact {
		t.Fatalf("result = %+v, want a plain successful send", res)
	}
}

func TestFirstContactIsMarkedAndStillGoesOut(t *testing.T) {
	var rec recorder
	q := newContactsQueue(t, time.Millisecond, nil, nil, rec.send)
	job, newContact := mustSubmit(t, q, "447700900000")
	if !newContact {
		t.Fatal("a recipient with no chat was not classed as a new contact")
	}
	res, _ := q.result(job.id)
	if !res.NewContact || res.State != sendQueued {
		t.Fatalf("booking = %+v, want a queued new contact", res)
	}
	res = waitFor(t, job.done)
	if !res.Success || !res.NewContact {
		t.Fatalf("result = %+v, want a successful send still marked new_contact", res)
	}
}

func TestNewContactsAreSpacedByTheirOwnGate(t *testing.T) {
	var rec recorder
	q := newContactsQueue(t, time.Hour, nil, nil, rec.send)

	// The idle gate lets the first one straight through; the second waits.
	first, _ := mustSubmit(t, q, "447700900000")
	second, _ := mustSubmit(t, q, "447700900001")
	waitFor(t, first.done)
	notSentWithin(t, second, 200*time.Millisecond)

	if st := q.status(); st.NewContactsHeld != 1 || st.Pending != 0 {
		t.Fatalf("status = %+v, want one new contact held and nothing on the main queue", st)
	}
	if got := rec.recipients(); len(got) != 1 || got[0] != "447700900000" {
		t.Fatalf("sent %v, want only the first new contact", got)
	}
}

// A two-message opener to one stranger is one new contact, not two: the second
// message must neither wait behind an hour of gate nor overtake the first.
func TestFollowUpToAHeldContactLeavesWithIt(t *testing.T) {
	var rec recorder
	// Unstarted: with the worker already running, an idle stage can release
	// "a" before the next line even submits "b" — submit everything the test
	// depends on first, then let the worker loose on all of it at once.
	q, start := newContactsQueueUnstarted(t, time.Hour, nil, rec.send)

	a, _ := mustSubmit(t, q, "447700900000")
	b, bNew := mustSubmit(t, q, "447700900000@s.whatsapp.net")
	c, _ := mustSubmit(t, q, "447700900001")
	if !bNew {
		t.Fatal("a second message to a still-held first contact must queue behind it in the stage")
	}
	start()
	waitFor(t, a.done)
	waitFor(t, b.done)
	notSentWithin(t, c, 200*time.Millisecond)

	if got := rec.recipients(); len(got) != 2 || got[0] != "447700900000" || got[1] != "447700900000@s.whatsapp.net" {
		t.Fatalf("sent %v, want both messages to the first contact, in order", got)
	}

	// Once released, the contact is no longer new for the rest of the session,
	// even though the fake store never learns of the chat.
	d, dNew := mustSubmit(t, q, "447700900000")
	if dNew {
		t.Fatal("a recipient already released was routed back through the stage")
	}
	waitFor(t, d.done)
}

func TestReachoutTimelockHoldsFirstContactsBack(t *testing.T) {
	var (
		mu     sync.Mutex
		active = true
	)
	lock := func() (bool, time.Time) {
		mu.Lock()
		defer mu.Unlock()
		return active, time.Now().Add(time.Hour)
	}
	var rec recorder
	q := newContactsQueue(t, time.Millisecond, []string{"447700900001@s.whatsapp.net"}, lock, rec.send)

	held, _ := mustSubmit(t, q, "447700900000")
	notSentWithin(t, held, 200*time.Millisecond)

	// An established chat is never affected by the lock.
	old, _ := mustSubmit(t, q, "447700900001")
	waitFor(t, old.done)
}

func TestReleaseAfterTheLockDoesNotBunchUp(t *testing.T) {
	// The gate is scheduled from the lock's end, not from now: two first
	// contacts waiting out a lock must not both leave the moment it lifts.
	ends := time.Now().Add(50 * time.Millisecond)
	lock := func() (bool, time.Time) { return time.Now().Before(ends), ends }
	var rec recorder
	q := newContactsQueue(t, time.Hour, nil, lock, rec.send)

	first, _ := mustSubmit(t, q, "447700900000")
	second, _ := mustSubmit(t, q, "447700900001")
	waitFor(t, first.done)
	notSentWithin(t, second, 200*time.Millisecond)
}

func TestShutdownWhileHeldAsNewContactStaysQueued(t *testing.T) {
	var rec recorder
	q := newSendQueue(newTestGate(0), rec.send)
	q.newContacts = newNewContactStage(newTestGate(time.Hour), func(string) bool { return true }, func() (bool, time.Time) { return false, time.Time{} })
	ctx, cancel := context.WithCancel(context.Background())
	go q.run(ctx)

	first, _ := mustSubmit(t, q, "447700900000")
	waitFor(t, first.done)
	var cleaned int32
	waiting, _, _ := q.submit(SendMessageRequest{Recipient: "447700900001"}, func() { atomic.AddInt32(&cleaned, 1) })
	queued, _, _ := q.submit(SendMessageRequest{Recipient: "447700900002"}, func() { atomic.AddInt32(&cleaned, 1) })
	time.Sleep(50 * time.Millisecond) // let the stage pick the first one up and start waiting
	cancel()

	// A restart must not fail these — they are still queued and will resume,
	// and their (hypothetical) uploads must not be cleaned up: the row on
	// disk stays exactly as submitted for the next start's recovery.
	for _, job := range []*sendJob{waiting, queued} {
		res := waitFor(t, job.done)
		if res.Success || res.State != sendQueued {
			t.Fatalf("result = %+v, want still queued, not failed", res)
		}
		if !strings.Contains(res.Message, "restart") {
			t.Fatalf("message %q does not explain the send will resume after a restart", res.Message)
		}
	}
	if got := rec.recipients(); len(got) != 1 {
		t.Fatalf("sent %v, want only the one that was released before shutdown", got)
	}
	if q.pending() != 0 {
		t.Fatalf("pending = %d after draining, want 0", q.pending())
	}
	if atomic.LoadInt32(&cleaned) != 0 {
		t.Fatal("a job kept queued for a restart must keep its upload, not clean it up")
	}
}

func TestFullNewContactStageRefuses(t *testing.T) {
	t.Setenv("WHATSAPP_SEND_MAX_QUEUE_DEPTH", "1")
	var rec recorder
	q := newSendQueue(newTestGate(0), rec.send)
	q.newContacts = newNewContactStage(newTestGate(time.Hour), func(string) bool { return true }, func() (bool, time.Time) { return false, time.Time{} })
	// No worker: the stage's buffer fills and stays full.
	if _, _, ok := q.submit(SendMessageRequest{Recipient: "1"}, func() {}); !ok {
		t.Fatal("first submit refused")
	}
	before := q.pending()
	if _, _, ok := q.submit(SendMessageRequest{Recipient: "2"}, func() {}); ok {
		t.Fatal("submit accepted past the stage's depth")
	}
	if got := q.pending(); got != before {
		t.Fatalf("pending = %d after a refusal, want %d", got, before)
	}
}

func TestDisabledNewContactGateMeansNoStage(t *testing.T) {
	t.Setenv("WHATSAPP_NEW_CONTACT_GAP_MEAN_SECONDS", "0")
	if s := newNewContactStage(newNewContactGateFromEnv(), func(string) bool { return true }, nil); s != nil {
		t.Fatal("a zero mean should disable the stage entirely")
	}
	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) { return true, "sent" })
	q.newContacts = nil
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)
	job, newContact := mustSubmit(t, q, "447700900000")
	if newContact {
		t.Fatal("with no stage, nothing is a new contact")
	}
	waitFor(t, job.done)
}

func TestNewContactGateDefaultsAndFloor(t *testing.T) {
	t.Setenv("WHATSAPP_NEW_CONTACT_GAP_MEAN_SECONDS", "")
	g := newNewContactGateFromEnv()
	if g.mean != defaultNewContactGapMean {
		t.Fatalf("mean = %s, want %s", g.mean, defaultNewContactGapMean)
	}
	for _, d := range drawN(g, 5000) {
		if d < minNewContactGap {
			t.Fatalf("gap %s is below the %s floor", d, minNewContactGap)
		}
	}
	t.Setenv("WHATSAPP_NEW_CONTACT_GAP_MEAN_SECONDS", "600")
	if g := newNewContactGateFromEnv(); g.mean != 10*time.Minute {
		t.Fatalf("mean = %s, want 10m", g.mean)
	}
}

func TestContactKeyIdentifiesPeopleNotChats(t *testing.T) {
	for _, tc := range []struct {
		recipient string
		want      string
		ok        bool
	}{
		{"447700900000", "447700900000@s.whatsapp.net", true},
		{"447700900000@s.whatsapp.net", "447700900000@s.whatsapp.net", true},
		{"447700900000:3@s.whatsapp.net", "447700900000@s.whatsapp.net", true},
		{"12345678901234@lid", "12345678901234@lid", true},
		{"120363000000000000@g.us", "", false},
		{"123@newsletter", "", false},
		{"status@broadcast", "", false},
		{"", "", false},
	} {
		got, ok := contactKey(tc.recipient)
		if ok != tc.ok || got != tc.want {
			t.Errorf("contactKey(%q) = %q, %v; want %q, %v", tc.recipient, got, ok, tc.want, tc.ok)
		}
	}
}

func TestIsNewContact(t *testing.T) {
	chats := fakeChats{known: map[string]bool{"447700900000@s.whatsapp.net": true, "12345678901234@lid": true}}
	for _, tc := range []struct {
		recipient string
		want      bool
	}{
		{"447700900000", false},
		{"447700900001", true},
		{"12345678901234@lid", false},
		{"99999999999999@lid", true},
		{"120363000000000000@g.us", false},
		{"not a jid", true}, // a bare string is a phone number to parseRecipient; the send fails on its own later
	} {
		if got := isNewContact(chats, tc.recipient); got != tc.want {
			t.Errorf("isNewContact(%q) = %v, want %v", tc.recipient, got, tc.want)
		}
	}
	if isNewContact(nil, "447700900001") {
		t.Fatal("with no store, nobody can be told apart, so nobody is new")
	}
}

// Due times are now computed once, at submission, from the gates' draws —
// not from a mean-based projection — and the reach-out time-lock is only
// checked once a job's turn actually arrives (see newContactStage.makeOnDue),
// so it plays no part in the estimate a submission is given up front. What
// submit can promise is bounded by the gate's own clamps.
func TestSubmissionEstimatesItsWait(t *testing.T) {
	var rec recorder
	q := newSendQueue(newTestGate(30*time.Second), rec.send)
	q.newContacts = newNewContactStage(newTestGate(30*time.Minute), func(string) bool { return true },
		func() (bool, time.Time) { return true, time.Now().Add(10 * time.Minute) })
	// No worker, so the positions are deterministic.

	_, first, _ := q.submit(SendMessageRequest{Recipient: "447700900000"}, func() {})
	_, second, _ := q.submit(SendMessageRequest{Recipient: "447700900001"}, func() {})
	if !first.newContact || first.ahead != 0 || second.ahead != 1 {
		t.Fatalf("positions = %+v, %+v", first, second)
	}
	// The stage is idle, so the first new contact's due time is immediate.
	if first.wait > time.Second {
		t.Fatalf("first wait = %s, want near-immediate (nothing else held)", first.wait)
	}
	// The second pays one new-contact gap on top of it.
	if second.wait < minNewContactGap || second.wait > gapMeanCeiling*30*time.Minute {
		t.Fatalf("second wait = %s, want one new-contact gap (%s..%s)", second.wait, minNewContactGap, gapMeanCeiling*30*time.Minute)
	}

	q.newContacts = nil
	_, a, _ := q.submit(SendMessageRequest{Recipient: "447700900002"}, func() {})
	_, b, _ := q.submit(SendMessageRequest{Recipient: "447700900003"}, func() {})
	if a.newContact || a.wait > time.Second || b.ahead != 1 {
		t.Fatalf("main-queue positions = %+v, %+v", a, b)
	}
	if b.wait < minGap || b.wait > gapMeanCeiling*30*time.Second {
		t.Fatalf("second main-queue wait = %s, want one main gap (%s..%s)", b.wait, minGap, gapMeanCeiling*30*time.Second)
	}
}

func TestQueuedMessageSpellsOutAFirstContact(t *testing.T) {
	plain := queuedMessage("snd-1", queuePosition{ahead: 2})
	if strings.Contains(plain, "First contact") {
		t.Fatalf("ordinary send described as a first contact: %q", plain)
	}
	held := queuedMessage("snd-2", queuePosition{ahead: 3, newContact: true, wait: 95 * time.Minute})
	for _, want := range []string{"snd-2", "First contact", "3 new contacts ahead", "1.6 h"} {
		if !strings.Contains(held, want) {
			t.Fatalf("%q lacks %q", held, want)
		}
	}
	for d, want := range map[time.Duration]string{
		20 * time.Second: "under a minute",
		29 * time.Minute: "29 min",
		3 * time.Hour:    "3.0 h",
	} {
		if got := roughDuration(d); got != want {
			t.Errorf("roughDuration(%s) = %q, want %q", d, got, want)
		}
	}
}
