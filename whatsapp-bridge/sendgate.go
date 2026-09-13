package main

import (
	"context"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// WhatsApp unlinked CT 119 mid-way through a burst of first-contact sends on
// 2026-09-13; a linked device that fires messages back-to-back looks like spam
// to their heuristics. Every outbound message therefore waits behind a randomly
// spaced gate: a fixed interval would be just as machine-like as no interval.
const (
	defaultGapMean      = 30 * time.Second
	defaultMaxQueueWait = 5 * time.Minute

	// A Poisson draw is a whole count and can come out 0, so the gap is floored
	// rather than left free to let two messages go out together.
	minGap = 1 * time.Second

	// Knuth's method multiplies uniforms down to exp(-lambda); far above this
	// that product underflows and the loop never terminates.
	maxPoissonLambda = 500
)

// sendGate serialises outbound sends, spacing them by a Poisson-distributed
// number of seconds. Callers reserve a slot and sleep until it comes round, so
// requests queue in arrival order rather than being rejected or sent early.
type sendGate struct {
	mu   sync.Mutex
	rand *rand.Rand
	next time.Time // earliest instant the next send may leave
	mean time.Duration
	// maxQueueWait bounds the backlog: a caller that would wait longer is
	// refused outright rather than held past any sane client timeout.
	maxQueueWait time.Duration
}

func newSendGateFromEnv() *sendGate {
	return &sendGate{
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		mean:         envDurationSeconds("WHATSAPP_SEND_GAP_MEAN_SECONDS", defaultGapMean),
		maxQueueWait: envDurationSeconds("WHATSAPP_SEND_MAX_QUEUE_WAIT_SECONDS", defaultMaxQueueWait),
	}
}

// envDurationSeconds reads a non-negative whole number of seconds; 0 is a
// meaningful value (it disables the gate), so only a malformed or negative
// setting falls back to the default.
func envDurationSeconds(key string, def time.Duration) time.Duration {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return def
	}
	return time.Duration(n) * time.Second
}

func (g *sendGate) enabled() bool { return g != nil && g.mean > 0 }

// draw returns the gap before the next send: Poisson(mean seconds), floored.
func (g *sendGate) draw() time.Duration {
	lambda := g.mean.Seconds()
	d := time.Duration(poisson(g.rand, lambda)) * time.Second
	if d < minGap {
		return minGap
	}
	return d
}

// poisson samples a Poisson count by Knuth's method, falling back to a normal
// approximation where that would underflow.
func poisson(r *rand.Rand, lambda float64) int {
	if lambda > maxPoissonLambda {
		n := int(math.Round(r.NormFloat64()*math.Sqrt(lambda) + lambda))
		if n < 0 {
			return 0
		}
		return n
	}
	limit := math.Exp(-lambda)
	p := 1.0
	for k := 0; ; k++ {
		p *= r.Float64()
		if p <= limit {
			return k
		}
	}
}

// reserve claims the next slot and returns when it falls due. ok is false if
// the backlog already exceeds maxQueueWait, in which case no slot is consumed.
func (g *sendGate) reserve(now time.Time) (slot time.Time, ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	slot = g.next
	if slot.Before(now) {
		slot = now
	}
	if g.maxQueueWait > 0 && slot.Sub(now) > g.maxQueueWait {
		return slot, false
	}
	g.next = slot.Add(g.draw())
	return slot, true
}

// wait blocks until this caller's slot comes round. It returns the time spent
// queued, and an error if the backlog is too deep or ctx is cancelled first.
func (g *sendGate) wait(ctx context.Context) (time.Duration, error) {
	if !g.enabled() {
		return 0, nil
	}
	now := time.Now()
	slot, ok := g.reserve(now)
	if !ok {
		return slot.Sub(now), errSendQueueFull
	}
	delay := slot.Sub(now)
	if delay <= 0 {
		return 0, nil
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return delay, ctx.Err()
	case <-t.C:
		return delay, nil
	}
}

type sendQueueFullError struct{}

func (sendQueueFullError) Error() string {
	return "send queue is too deep; rate limit holds outbound messages apart"
}

var errSendQueueFull = sendQueueFullError{}
