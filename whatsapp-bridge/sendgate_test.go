package main

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"
)

func newTestGate(mean, maxQueueWait time.Duration) *sendGate {
	return &sendGate{
		rand:         rand.New(rand.NewSource(1)),
		mean:         mean,
		maxQueueWait: maxQueueWait,
	}
}

func TestFirstSendIsNotDelayed(t *testing.T) {
	g := newTestGate(30*time.Second, defaultMaxQueueWait)
	queued, err := g.wait(context.Background())
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if queued != 0 {
		t.Fatalf("idle gate delayed the first send by %s", queued)
	}
}

func TestSlotsAreSpacedByAtLeastMinGap(t *testing.T) {
	g := newTestGate(30*time.Second, 0)
	now := time.Now()
	prev, _ := g.reserve(now)
	for i := 0; i < 200; i++ {
		slot, ok := g.reserve(now)
		if !ok {
			t.Fatalf("reserve %d refused with no queue cap", i)
		}
		if gap := slot.Sub(prev); gap < minGap {
			t.Fatalf("slots %d and %d are %s apart, below the %s floor", i, i+1, gap, minGap)
		}
		prev = slot
	}
}

func TestGapMeanMatchesConfiguredLambda(t *testing.T) {
	const n = 20000
	mean := 30 * time.Second
	g := newTestGate(mean, 0)

	var total time.Duration
	for i := 0; i < n; i++ {
		total += g.draw()
	}
	got := total.Seconds() / n
	// Poisson(30) has sd 30**0.5, so the sample mean's sd is well under 0.1 s
	// at this n; the floor lifts it a shade, never lowers it.
	if math.Abs(got-mean.Seconds()) > 0.5 {
		t.Fatalf("mean gap %.2fs, want ~%.0fs", got, mean.Seconds())
	}
}

func TestGapIsNotConstant(t *testing.T) {
	g := newTestGate(30*time.Second, 0)
	first := g.draw()
	for i := 0; i < 100; i++ {
		if g.draw() != first {
			return
		}
	}
	t.Fatal("every gap came out identical; a fixed interval is as machine-like as none")
}

func TestQueueRefusesOnceBacklogExceedsCap(t *testing.T) {
	g := newTestGate(30*time.Second, time.Minute)
	now := time.Now()

	refusedAt := -1
	for i := 0; i < 100; i++ {
		if _, ok := g.reserve(now); !ok {
			refusedAt = i
			break
		}
	}
	if refusedAt < 1 {
		t.Fatalf("refused at reservation %d; the first sends must be allowed through", refusedAt)
	}

	// A refusal must not consume a slot, or a rejected caller would still push
	// the queue out for everyone behind it.
	before := g.next
	if _, ok := g.reserve(now); ok {
		t.Fatal("gate allowed a reservation after refusing one at the same instant")
	}
	if !g.next.Equal(before) {
		t.Fatal("a refused reservation advanced the queue")
	}
}

func TestWaitReturnsQueueFullError(t *testing.T) {
	g := newTestGate(30*time.Second, time.Nanosecond)
	if _, err := g.wait(context.Background()); err != nil {
		t.Fatalf("first send refused: %v", err)
	}
	_, err := g.wait(context.Background())
	if err != errSendQueueFull {
		t.Fatalf("err = %v, want errSendQueueFull", err)
	}
}

func TestWaitAbandonsQueueWhenCallerGoesAway(t *testing.T) {
	g := newTestGate(30*time.Second, 0)
	if _, err := g.wait(context.Background()); err != nil {
		t.Fatalf("first send refused: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.wait(ctx); err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestDisabledGateNeverWaits(t *testing.T) {
	g := newTestGate(0, defaultMaxQueueWait)
	for i := 0; i < 5; i++ {
		queued, err := g.wait(context.Background())
		if err != nil || queued != 0 {
			t.Fatalf("disabled gate queued %s (err %v)", queued, err)
		}
	}
}

func TestEnvDurationSeconds(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		want        time.Duration
	}{
		{"unset", "", 30 * time.Second},
		{"set", "45", 45 * time.Second},
		{"zero disables", "0", 0},
		{"negative falls back", "-5", 30 * time.Second},
		{"malformed falls back", "soon", 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value != "" {
				t.Setenv("WHATSAPP_SEND_GAP_MEAN_SECONDS", tc.value)
			}
			if got := envDurationSeconds("WHATSAPP_SEND_GAP_MEAN_SECONDS", 30*time.Second); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestPoissonApproximationForLargeLambda(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	const lambda = maxPoissonLambda * 2
	total := 0
	for i := 0; i < 2000; i++ {
		total += poisson(r, lambda)
	}
	got := float64(total) / 2000
	if math.Abs(got-lambda) > 5 {
		t.Fatalf("mean %.1f, want ~%d", got, lambda)
	}
}
