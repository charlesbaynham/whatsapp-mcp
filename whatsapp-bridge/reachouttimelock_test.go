package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.mau.fi/util/jsontime"
	"go.mau.fi/whatsmeow/types/events"
)

// --- notification handling ---

func TestReachoutTimelockNotificationActiveWithEnds(t *testing.T) {
	lock := &reachoutTimelock{}
	ends := time.Now().Add(10 * time.Minute).Truncate(time.Second)
	evt := &events.NotifyAccountReachoutTimelock{
		IsActive:            true,
		EnforcementType:     "hard",
		TimeEnforcementEnds: jsontime.UnixString{Time: ends},
	}
	handleReachoutTimelockNotification(lock, evt, noopLog)

	active, gotEnds := lock.activeNow()
	if !active {
		t.Fatal("lock reported inactive after an active notification")
	}
	if !gotEnds.Equal(ends) {
		t.Fatalf("ends = %v, want %v", gotEnds, ends)
	}
	st := lock.state()
	if st.EnforcementType != "hard" {
		t.Fatalf("enforcement_type = %q, want hard", st.EnforcementType)
	}
}

func TestReachoutTimelockNotificationActiveWithoutEndsDefaultsTo60s(t *testing.T) {
	lock := &reachoutTimelock{}
	before := time.Now()
	evt := &events.NotifyAccountReachoutTimelock{IsActive: true}
	handleReachoutTimelockNotification(lock, evt, noopLog)

	active, ends := lock.activeNow()
	if !active {
		t.Fatal("lock reported inactive after an active notification")
	}
	wantEarliest := before.Add(defaultTimelockDuration)
	wantLatest := time.Now().Add(defaultTimelockDuration)
	if ends.Before(wantEarliest) || ends.After(wantLatest) {
		t.Fatalf("ends = %v, want within [%v, %v]", ends, wantEarliest, wantLatest)
	}
}

func TestReachoutTimelockNotificationLifted(t *testing.T) {
	lock := &reachoutTimelock{}
	lock.set(true, "hard", time.Now().Add(time.Hour), "notification")

	handleReachoutTimelockNotification(lock, &events.NotifyAccountReachoutTimelock{IsActive: false}, noopLog)

	if active, _ := lock.activeNow(); active {
		t.Fatal("lock still reports active after a lifted notification")
	}
	if st := lock.state(); st.Active {
		t.Fatal("state().Active true after a lifted notification")
	}
}

func TestReachoutTimelockActiveNowExpires(t *testing.T) {
	lock := &reachoutTimelock{}
	lock.set(true, "hard", time.Now().Add(-time.Second), "notification")

	if active, _ := lock.activeNow(); active {
		t.Fatal("a lock whose end has already passed still reports active")
	}
}

// --- MEX query response parsing ---

type fakeMexQuerier struct {
	raw json.RawMessage
	err error
}

func (f *fakeMexQuerier) SendMexIQ(context.Context, string, any) (json.RawMessage, error) {
	return f.raw, f.err
}

func TestQueryReachoutTimelockActiveWithEnds(t *testing.T) {
	q := &fakeMexQuerier{raw: json.RawMessage(`{"xwa2_fetch_account_reachout_timelock":{"is_active":true,"time_enforcement_ends":"1999999999","enforcement_type":"soft"}}`)}
	active, enforcementType, ends, err := queryReachoutTimelock(context.Background(), q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !active || enforcementType != "soft" {
		t.Fatalf("active=%v enforcementType=%q, want true, soft", active, enforcementType)
	}
	if want := time.Unix(1999999999, 0); !ends.Equal(want) {
		t.Fatalf("ends = %v, want %v", ends, want)
	}
}

func TestQueryReachoutTimelockInactive(t *testing.T) {
	q := &fakeMexQuerier{raw: json.RawMessage(`{"xwa2_fetch_account_reachout_timelock":{"is_active":false}}`)}
	active, _, ends, err := queryReachoutTimelock(context.Background(), q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if active {
		t.Fatal("reported active for an inactive response")
	}
	if !ends.IsZero() {
		t.Fatalf("ends = %v, want zero", ends)
	}
}

func TestQueryReachoutTimelockZeroEndsString(t *testing.T) {
	q := &fakeMexQuerier{raw: json.RawMessage(`{"xwa2_fetch_account_reachout_timelock":{"is_active":true,"time_enforcement_ends":"0"}}`)}
	active, _, ends, err := queryReachoutTimelock(context.Background(), q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !active {
		t.Fatal("reported inactive for an active response")
	}
	if !ends.IsZero() {
		t.Fatalf("ends = %v, want zero for a \"0\" time_enforcement_ends", ends)
	}
}

func TestQueryReachoutTimelockMalformedJSON(t *testing.T) {
	q := &fakeMexQuerier{raw: json.RawMessage(`not json`)}
	if _, _, _, err := queryReachoutTimelock(context.Background(), q); err == nil {
		t.Fatal("expected an error for malformed JSON")
	}
}

func TestQueryReachoutTimelockMalformedEndsField(t *testing.T) {
	q := &fakeMexQuerier{raw: json.RawMessage(`{"xwa2_fetch_account_reachout_timelock":{"is_active":true,"time_enforcement_ends":"soon"}}`)}
	if _, _, _, err := queryReachoutTimelock(context.Background(), q); err == nil {
		t.Fatal("expected an error for a non-numeric time_enforcement_ends")
	}
}

func TestQueryReachoutTimelockMissingField(t *testing.T) {
	q := &fakeMexQuerier{raw: json.RawMessage(`{"some_other_field":{}}`)}
	if _, _, _, err := queryReachoutTimelock(context.Background(), q); err == nil {
		t.Fatal("expected an error when the expected field is absent")
	}
}

func TestQueryReachoutTimelockPropagatesQueryError(t *testing.T) {
	q := &fakeMexQuerier{err: errors.New("iq timed out")}
	if _, _, _, err := queryReachoutTimelock(context.Background(), q); err == nil {
		t.Fatal("expected the query error to propagate")
	}
}

// --- refusal logic ---

func TestRefuseFirstContactDuringTimelock(t *testing.T) {
	activeLock := &reachoutTimelock{}
	activeLock.set(true, "hard", time.Now().Add(time.Minute), "notification")

	expiredLock := &reachoutTimelock{}
	expiredLock.set(true, "hard", time.Now().Add(-time.Minute), "notification")

	cases := []struct {
		name         string
		firstContact bool
		lock         *reachoutTimelock
		wantRefuse   bool
	}{
		{"first contact, active lock", true, activeLock, true},
		{"established chat, active lock", false, activeLock, false},
		{"first contact, expired lock", true, expiredLock, false},
		{"first contact, never-set lock", true, &reachoutTimelock{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			refuse, reason := refuseFirstContactDuringTimelock(c.firstContact, c.lock)
			if refuse != c.wantRefuse {
				t.Fatalf("refuse = %v, want %v (reason: %q)", refuse, c.wantRefuse, reason)
			}
			if refuse && reason == "" {
				t.Fatal("refused with no reason given")
			}
		})
	}
}

func TestIsReachoutTimelockSendError(t *testing.T) {
	if !isReachoutTimelockSendError(errors.New("failed to send message: server returned error 463")) {
		t.Fatal("did not recognise a 463 send error")
	}
	if isReachoutTimelockSendError(errors.New("server returned error 500")) {
		t.Fatal("misidentified an unrelated server error as a 463")
	}
	if isReachoutTimelockSendError(nil) {
		t.Fatal("nil error reported as a 463")
	}
}
