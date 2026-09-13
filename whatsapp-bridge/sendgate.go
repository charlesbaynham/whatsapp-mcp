package main

import (
	"math/rand"
	"os"
	"strconv"
	"time"
)

// A linked device sending back-to-back reads as a spammer and gets unlinked
// (2026-09-13); the exponential gap is a Poisson process's inter-arrival time,
// which has the shape of someone typing rather than of a metronome.
const (
	defaultGapMean = 30 * time.Second

	// An exponential draw is unbounded at both ends: the floor stops two
	// messages leaving together, the ceiling stops a freak draw parking the
	// queue for an hour.
	minGap         = 1 * time.Second
	gapMeanCeiling = 10
)

// sendGate spaces sends by Exponential(1/mean). It is driven by the queue's
// single worker, so it needs no locking of its own.
type sendGate struct {
	rand *rand.Rand
	mean time.Duration
	next time.Time // earliest instant the next send may leave
}

func newSendGateFromEnv() *sendGate {
	return &sendGate{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		mean: envDurationSeconds("WHATSAPP_SEND_GAP_MEAN_SECONDS", defaultGapMean),
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

// envPositiveInt reads a whole number above zero; anything else is the default.
func envPositiveInt(key string, def int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil || n < 1 {
		return def
	}
	return n
}

func (g *sendGate) enabled() bool { return g != nil && g.mean > 0 }

// draw returns the gap before the next send, clamped at both ends.
func (g *sendGate) draw() time.Duration {
	d := time.Duration(g.rand.ExpFloat64() * float64(g.mean))
	if d < minGap {
		return minGap
	}
	if ceiling := gapMeanCeiling * g.mean; d > ceiling {
		return ceiling
	}
	return d
}

// due returns the instant this send may go out, and records the gap before the
// one after it. An idle gate is due immediately.
func (g *sendGate) due(now time.Time) time.Time {
	if !g.enabled() {
		return now
	}
	slot := g.next
	if slot.Before(now) {
		slot = now
	}
	g.next = slot.Add(g.draw())
	return slot
}
