package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types"
)

// newContactStage holds sends to people this account has never messaged,
// releasing them onto the main send queue one recipient at a time, spaced by
// its own, much wider gate.
//
// The ordinary gate keeps any two sends apart by ~30 s, which is enough to not
// look like a burst. It is not enough for first contacts: to WhatsApp's spam
// heuristics, an account that starts conversations with a run of strangers —
// as happens when the same note goes to everyone in a group — is the profile
// of a spammer, and the account gets its reach-out time-lock or is unlinked.
// Spreading those first contacts over hours is what keeps the rate of *new*
// conversations low, whatever the rate of messages.
//
// Only the first message to a recipient pays the wait. Follow-ups to someone
// already released in this session go straight to the main queue, and ones
// submitted while their first message is still held queue behind it and
// leave with it, so a multi-message opener reads as one new contact.
type newContactStage struct {
	jobs  chan *sendJob
	gate  *sendGate
	isNew func(recipient string) bool
	// lock reports whether WhatsApp's reach-out time-lock is active and when
	// it ends. Releasing a first contact into it would only fail the send,
	// so the stage waits it out instead.
	lock func() (active bool, ends time.Time)

	mu       sync.Mutex
	released map[string]bool // recipients let through this session, by canonical JID
}

func newNewContactStage(gate *sendGate, isNew func(string) bool, lock func() (bool, time.Time)) *newContactStage {
	if !gate.enabled() {
		return nil
	}
	return &newContactStage{
		jobs:     make(chan *sendJob, queueDepthFromEnv()),
		gate:     gate,
		isNew:    isNew,
		lock:     lock,
		released: make(map[string]bool),
	}
}

// holds reports whether a send to recipient belongs in this stage: a first
// contact that has not already been released this session.
func (s *newContactStage) holds(recipient string) bool {
	key, ok := contactKey(recipient)
	if !ok {
		return false
	}
	s.mu.Lock()
	seen := s.released[key]
	s.mu.Unlock()
	return !seen && s.isNew(recipient)
}

// run drives the stage until ctx is cancelled: one goroutine, so the gate
// needs no lock.
func (s *newContactStage) run(ctx context.Context, q *sendQueue) {
	for {
		select {
		case <-ctx.Done():
			s.drain(q)
			return
		case job := <-s.jobs:
			s.release(ctx, q, job)
		}
	}
}

// release waits for the recipient's turn, then hands the job to the main
// queue. A recipient already released — the earlier message of a pair — is
// handed straight on, keeping its messages in order without spending a gap.
func (s *newContactStage) release(ctx context.Context, q *sendQueue, job *sendJob) {
	key, _ := contactKey(job.req.Recipient)
	if !s.alreadyReleased(key) {
		if !s.wait(ctx, job) {
			job.cleanup()
			q.promote(job)
			q.abandon(job)
			return
		}
		s.markReleased(key)
	}

	q.promote(job)
	select {
	case q.jobs <- job:
	case <-ctx.Done():
		job.cleanup()
		q.abandon(job)
	}
}

// wait blocks until the gate's next slot, pushed back past the end of any
// reach-out time-lock in force. It reports false if the bridge stops first.
func (s *newContactStage) wait(ctx context.Context, job *sendJob) bool {
	from := time.Now()
	if active, ends := s.lock(); active && ends.After(from) {
		fmt.Printf("Holding new-contact send %s to %s until the reach-out time-lock ends at %s\n",
			job.id, job.req.Recipient, ends.Format(time.RFC3339))
		from = ends
	}
	wait := time.Until(s.gate.due(from))
	if wait <= 0 {
		return true
	}
	fmt.Printf("Holding new-contact send %s to %s for %s (new-contact spacing)\n",
		job.id, job.req.Recipient, wait.Round(time.Second))
	t := time.NewTimer(wait)
	select {
	case <-ctx.Done():
		t.Stop()
		return false
	case <-t.C:
		return true
	}
}

// expectedWait estimates how long a first contact submitted behind `ahead`
// others in the stage waits before it is released: until the gate's next
// slot or the end of any reach-out time-lock, whichever is later, then one
// mean gap per contact ahead of it. Follow-ups to a recipient already in the
// stage count as contacts here although they spend no gap, so this errs long.
func (s *newContactStage) expectedWait(ahead int) time.Duration {
	start := s.gate.peek()
	if active, ends := s.lock(); active && ends.After(start) {
		start = ends
	}
	return waitFrom(start) + time.Duration(ahead)*s.gate.mean
}

func (s *newContactStage) alreadyReleased(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.released[key]
}

func (s *newContactStage) markReleased(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released[key] = true
}

// drain fails everything still held when the bridge stops, as the main
// queue's drain does for its own backlog.
func (s *newContactStage) drain(q *sendQueue) {
	for {
		select {
		case job := <-s.jobs:
			job.cleanup()
			q.promote(job)
			q.abandon(job)
		default:
			return
		}
	}
}

// contactKey is the identity a recipient is tracked under: its canonical
// non-device JID, so "447700900000" and "447700900000@s.whatsapp.net" are the
// same person. ok is false for a recipient that is not a person at all —
// a group, a broadcast list, a newsletter — or that does not parse; those
// are never first contacts in the sense that matters here.
func contactKey(recipient string) (key string, ok bool) {
	jid, err := parseRecipient(recipient)
	if err != nil || jid.User == "" {
		return "", false
	}
	if jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer {
		return "", false
	}
	return jid.ToNonAD().String(), true
}

// isNewContact reports whether recipient is a person this bridge has never
// had a chat with. Unlike needsRegistrationCheck, a LID-addressed recipient
// counts too: a group member messaged privately for the first time is exactly
// the stranger this stage exists for, whichever way they are addressed.
func isNewContact(chats chatExistenceChecker, recipient string) bool {
	jid, err := parseRecipient(recipient)
	if err != nil || jid.User == "" {
		return false
	}
	if jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer {
		return false
	}
	return isFirstContact(chats, jid)
}
