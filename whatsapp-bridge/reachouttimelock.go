package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// defaultTimelockDuration is Baileys' own fallback: when WhatsApp reports the
// lock active but does not say when it ends, treat it as ending in 60s.
const defaultTimelockDuration = 60 * time.Second

// reachoutTimelockQueryID is the MEX query id WA Web uses to fetch the
// account's reach-out time-lock state (WAWebMexFetchReachoutTimelockJobQuery).
const reachoutTimelockQueryID = "23983697327930364"

// reachoutTimelock tracks WhatsApp's server-side "reach-out time-lock" — the
// rate limit behind error 463 on a first-contact send. WhatsApp reports it two
// ways: an unsolicited mex notification ("notification") and an on-demand MEX
// query ("query"); a 463 on a send that predates either report is a third,
// best-effort signal ("send-463").
type reachoutTimelock struct {
	mu              sync.Mutex
	active          bool
	enforcementType string
	ends            time.Time
	updatedAt       time.Time
	source          string
}

// reachoutTimelockState is the JSON shape exposed at /api/status and
// /api/reachout-timelock.
type reachoutTimelockState struct {
	Active          bool       `json:"active"`
	EnforcementType string     `json:"enforcement_type"`
	Ends            *time.Time `json:"ends"`
	CheckedAt       *time.Time `json:"checked_at"`
}

// set records a report of the lock's state. An active report with no end
// time is given Baileys' 60s default, so every other reader can treat a zero
// ends as "not currently active" rather than special-casing it.
func (r *reachoutTimelock) set(active bool, enforcementType string, ends time.Time, source string) {
	if active && ends.IsZero() {
		ends = time.Now().Add(defaultTimelockDuration)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active = active
	r.enforcementType = enforcementType
	r.ends = ends
	r.updatedAt = time.Now()
	r.source = source
}

// activeNow reports whether the lock is active right now: a report of
// "active" whose ends has since passed no longer blocks anything.
func (r *reachoutTimelock) activeNow() (bool, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active && time.Now().Before(r.ends) {
		return true, r.ends
	}
	return false, time.Time{}
}

func (r *reachoutTimelock) state() reachoutTimelockState {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := reachoutTimelockState{
		Active:          r.active && time.Now().Before(r.ends),
		EnforcementType: r.enforcementType,
	}
	if !r.ends.IsZero() {
		ends := r.ends
		st.Ends = &ends
	}
	if !r.updatedAt.IsZero() {
		checkedAt := r.updatedAt
		st.CheckedAt = &checkedAt
	}
	return st
}

// handleReachoutTimelockNotification updates the tracked lock from the mex
// notification whatsmeow dispatches as *events.NotifyAccountReachoutTimelock,
// and logs the transition.
func handleReachoutTimelockNotification(lock *reachoutTimelock, evt *events.NotifyAccountReachoutTimelock, logger waLog.Logger) {
	lock.set(evt.IsActive, evt.EnforcementType, evt.TimeEnforcementEnds.Time, "notification")
	if !evt.IsActive {
		logger.Infof("Reach-out time-lock lifted")
		return
	}
	_, ends := lock.activeNow()
	logger.Infof("Reach-out time-lock active until %s (%s)", ends.Format(time.RFC3339), evt.EnforcementType)
}

// mexQuerier is the one method of *whatsmeow.DangerousInternalClient this
// needs, named separately so the response parsing is unit-testable with a
// fake instead of a live WhatsApp connection.
type mexQuerier interface {
	SendMexIQ(ctx context.Context, queryID string, variables any) (json.RawMessage, error)
}

// reachoutTimelockQueryResult is the shape of the "data" object a
// reachoutTimelockQueryID MEX query returns: the same envelope newsletter.go
// consumes, keyed by the query's result field.
type reachoutTimelockQueryResult struct {
	Result *struct {
		IsActive            bool   `json:"is_active"`
		TimeEnforcementEnds string `json:"time_enforcement_ends"`
		EnforcementType     string `json:"enforcement_type"`
	} `json:"xwa2_fetch_account_reachout_timelock"`
}

// parseReachoutTimelockResponse decodes the "data" object a
// reachoutTimelockQueryID query returns. time_enforcement_ends is absent or
// "0" when there is no lock end to report.
func parseReachoutTimelockResponse(raw json.RawMessage) (active bool, enforcementType string, ends time.Time, err error) {
	var result reachoutTimelockQueryResult
	if jsonErr := json.Unmarshal(raw, &result); jsonErr != nil {
		return false, "", time.Time{}, fmt.Errorf("malformed reach-out time-lock response: %w", jsonErr)
	}
	if result.Result == nil {
		return false, "", time.Time{}, fmt.Errorf("reach-out time-lock response missing xwa2_fetch_account_reachout_timelock")
	}
	if s := result.Result.TimeEnforcementEnds; s != "" && s != "0" {
		secs, convErr := strconv.ParseInt(s, 10, 64)
		if convErr != nil {
			return false, "", time.Time{}, fmt.Errorf("malformed time_enforcement_ends %q: %w", s, convErr)
		}
		ends = time.Unix(secs, 0)
	}
	return result.Result.IsActive, result.Result.EnforcementType, ends, nil
}

// queryReachoutTimelock asks WhatsApp directly for the account's current
// reach-out time-lock state.
func queryReachoutTimelock(ctx context.Context, q mexQuerier) (active bool, enforcementType string, ends time.Time, err error) {
	raw, err := q.SendMexIQ(ctx, reachoutTimelockQueryID, map[string]any{})
	if err != nil {
		return false, "", time.Time{}, err
	}
	return parseReachoutTimelockResponse(raw)
}

// refuseFirstContactDuringTimelock decides whether a send must be refused to
// avoid spending the account's reach-out budget: only a first-contact send
// while the lock is active is refused, never a message into an established
// chat.
func refuseFirstContactDuringTimelock(firstContact bool, lock *reachoutTimelock) (refuse bool, reason string) {
	if !firstContact {
		return false, ""
	}
	active, ends := lock.activeNow()
	if !active {
		return false, ""
	}
	return true, fmt.Sprintf(
		"Refusing to send: reach-out time-lock active until %s; first-contact sends would spend the account's reach-out budget",
		ends.Format(time.RFC3339))
}

// isReachoutTimelockSendError reports whether a send failed with WhatsApp's
// error 463 (SenderReachoutTimelocked) — whatsmeow's own message for it is
// "server returned error 463", from its ErrServerReturnedError sentinel.
func isReachoutTimelockSendError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "error 463")
}
