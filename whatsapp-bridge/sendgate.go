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

	// Reaching out to people this account has never messaged is the signature
	// WhatsApp's spam heuristics weight most heavily — the same text to a run
	// of strangers is what a spammer looks like — so first contacts are spaced
	// far more widely than messages into established chats. The floor keeps a
	// short draw from putting two strangers within a minute of each other.
	defaultNewContactGapMean = 30 * time.Minute
	minNewContactGap         = 1 * time.Minute
)

// sendGate is the exponential draw a scheduledQueue uses to space its sends:
// Exponential(1/mean), clamped at both ends. It carries no state of its own
// beyond the RNG — the "when did the last one leave" bookkeeping now lives on
// scheduledQueue (as lastDue), because that is what gets persisted and
// recovered across a restart; the gate is just where the randomness comes
// from.
type sendGate struct {
	rand  *rand.Rand
	mean  time.Duration
	floor time.Duration // shortest gap a draw can produce
}

func newSendGate(mean, floor time.Duration) *sendGate {
	return &sendGate{
		rand:  rand.New(rand.NewSource(time.Now().UnixNano())),
		mean:  mean,
		floor: floor,
	}
}

func newSendGateFromEnv() *sendGate {
	return newSendGate(envDurationSeconds("WHATSAPP_SEND_GAP_MEAN_SECONDS", defaultGapMean), minGap)
}

// newNewContactGateFromEnv builds the gate that spaces first-contact sends.
// A mean of 0 disables the stage: first contacts then go straight onto the
// main queue and are spaced only by the ordinary gate.
func newNewContactGateFromEnv() *sendGate {
	return newSendGate(envDurationSeconds("WHATSAPP_NEW_CONTACT_GAP_MEAN_SECONDS", defaultNewContactGapMean), minNewContactGap)
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
	if d < g.floor {
		return g.floor
	}
	if ceiling := gapMeanCeiling * g.mean; d > ceiling {
		return ceiling
	}
	return d
}
