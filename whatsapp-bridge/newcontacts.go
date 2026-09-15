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
// its own, much wider gate. It is a thin layer of per-recipient rules over a
// scheduledQueue (see scheduledqueue.go): the queue itself only knows about
// due times, ordering and depth; this layer knows what a "recipient" is and
// which of them have already been let through.
//
// The ordinary gate keeps any two sends apart by ~30 s, which is enough to not
// look like a burst. It is not enough for first contacts: to WhatsApp's spam
// heuristics, an account that starts conversations with a run of strangers —
// as happens when the same note goes to everyone in a group — is the profile
// of a spammer, and the account gets its reach-out time-lock or is unlinked.
// Spreading those first contacts over hours is what keeps the rate of *new*
// conversations low, whatever the rate of messages.
//
// Only the first message to a recipient pays the wait: it gets a freshly
// scheduled due time. Follow-ups submitted while it is still held share that
// same due time (see submit) and leave with it, so a multi-message opener
// reads as one new contact; once someone has been released (or, after a
// restart, once a chat with them exists) they are an ordinary recipient.
type newContactStage struct {
	stage *scheduledQueue
	isNew func(recipient string) bool
	// lock reports whether WhatsApp's reach-out time-lock is active and when
	// it ends. Releasing a first contact into it would only fail the send,
	// so the stage waits it out instead.
	lock func() (active bool, ends time.Time)

	mu       sync.Mutex
	released map[string]bool      // recipients let through this session, by canonical JID
	heldDue  map[string]time.Time // recipient -> the due time their first still-held message got
}

func newNewContactStage(gate *sendGate, isNew func(string) bool, lock func() (bool, time.Time)) *newContactStage {
	if !gate.enabled() {
		return nil
	}
	return &newContactStage{
		stage:    newScheduledQueue(stageNewContact, gate, queueDepthFromEnv(), nil),
		isNew:    isNew,
		lock:     lock,
		released: make(map[string]bool),
		heldDue:  make(map[string]time.Time),
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

// submit places job in the stage: a brand-new recipient gets a freshly
// scheduled due time; a recipient whose first message is still held here
// shares that due time and spends no gap of its own. established reports
// which happened, so a caller whose persistence then fails knows whether to
// clear the heldDue entry this call just created.
func (s *newContactStage) submit(job *sendJob, queuedAt time.Time) (dueAt time.Time, ahead int, ok bool, established bool) {
	key, _ := contactKey(job.req.Recipient)
	s.mu.Lock()
	shared, isHeld := s.heldDue[key]
	s.mu.Unlock()
	if isHeld {
		ahead, ok = s.stage.insertShared(job, shared, queuedAt)
		return shared, ahead, ok, false
	}
	dueAt, ahead, ok = s.stage.schedule(job, queuedAt)
	if ok {
		s.mu.Lock()
		s.heldDue[key] = dueAt
		s.mu.Unlock()
	}
	return dueAt, ahead, ok, true
}

// unsubmit undoes submit for a job whose row then failed to persist.
func (s *newContactStage) unsubmit(job *sendJob, established bool) {
	s.stage.remove(job.id)
	if !established {
		return
	}
	key, _ := contactKey(job.req.Recipient)
	s.mu.Lock()
	delete(s.heldDue, key)
	s.mu.Unlock()
}

// trackHeld seeds heldDue for a row recovered from disk that is still held
// in this stage, so a follow-up submitted before it is released shares its
// due time exactly as it would have before the restart.
func (s *newContactStage) trackHeld(r persistedRow) {
	key, ok := contactKey(r.Recipient)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, seen := s.heldDue[key]; !seen || r.DueAt.Before(existing) {
		s.heldDue[key] = r.DueAt
	}
}

// makeOnDue builds the stage's onDue: wait out the reach-out time-lock if
// this is the recipient's first (not yet released) message, then hand the
// job on to the main queue's own schedule. main is the scheduledQueue behind
// the ordinary send queue; stillQueued answers a job's caller without
// touching its row when the bridge stops mid-wait or mid-handoff.
func (s *newContactStage) makeOnDue(main *scheduledQueue, stillQueued func(*sendJob)) func(context.Context, *sendJob) {
	return func(ctx context.Context, job *sendJob) {
		if ctx.Err() != nil {
			stillQueued(job)
			return
		}
		key, _ := contactKey(job.req.Recipient)
		if !s.alreadyReleased(key) {
			if active, ends := s.lock(); active {
				if wait := time.Until(ends); wait > 0 {
					fmt.Printf("Holding new-contact send %s to %s until the reach-out time-lock ends at %s\n",
						job.id, job.req.Recipient, ends.Format(time.RFC3339))
					t := time.NewTimer(wait)
					select {
					case <-ctx.Done():
						t.Stop()
						stillQueued(job)
						return
					case <-t.C:
					}
					// Everyone still held behind this one was scheduled
					// assuming no delay; push them back by exactly the delay
					// actually incurred so they do not all leave in a bunch
					// the moment the lock lifts.
					s.stage.shiftFrom(0, wait)
				}
			}
			s.markReleased(key)
		}
		for {
			if _, ok := main.relocate(job); ok {
				return
			}
			select {
			case <-ctx.Done():
				stillQueued(job)
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
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
	delete(s.heldDue, key)
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
