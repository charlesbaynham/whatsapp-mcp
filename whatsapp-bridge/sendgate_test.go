package main

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func newTestGate(mean time.Duration) *sendGate {
	return &sendGate{rand: rand.New(rand.NewSource(1)), mean: mean}
}

func drawN(g *sendGate, n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = g.draw()
	}
	return out
}

func TestFirstSendIsDueImmediately(t *testing.T) {
	g := newTestGate(30 * time.Second)
	now := time.Now()
	if due := g.due(now); due.After(now) {
		t.Fatalf("idle gate held the first send back by %s", due.Sub(now))
	}
}

func TestGapMeanMatchesConfiguredMean(t *testing.T) {
	mean := 30 * time.Second
	got := meanSeconds(drawN(newTestGate(mean), 50000))
	// An exponential's sd equals its mean, so the sample mean's sd here is
	// ~0.13 s; the clamps pull it in slightly at both ends.
	if math.Abs(got-mean.Seconds()) > 1.5 {
		t.Fatalf("mean gap %.2fs, want ~%.0fs", got, mean.Seconds())
	}
}

func TestExponentialGapsAreMostlyShorterThanTheMean(t *testing.T) {
	mean := 30 * time.Second
	gaps := drawN(newTestGate(mean), 10000)
	short := 0
	for _, d := range gaps {
		if d < mean {
			short++
		}
	}
	// 1 - 1/e ~ 63% of an exponential's mass is below its mean. This is what
	// distinguishes it from the Poisson draw it replaced, which clustered.
	if frac := float64(short) / float64(len(gaps)); frac < 0.55 || frac > 0.70 {
		t.Fatalf("%.0f%% of gaps below the mean, want ~63%%", frac*100)
	}
}

func TestGapsAreClampedAtBothEnds(t *testing.T) {
	mean := 30 * time.Second
	ceiling := gapMeanCeiling * mean
	for _, d := range drawN(newTestGate(mean), 20000) {
		if d < minGap {
			t.Fatalf("gap %s is below the %s floor", d, minGap)
		}
		if d > ceiling {
			t.Fatalf("gap %s is above the %s ceiling", d, ceiling)
		}
	}
}

func TestGapIsNotConstant(t *testing.T) {
	g := newTestGate(30 * time.Second)
	first := g.draw()
	for i := 0; i < 100; i++ {
		if g.draw() != first {
			return
		}
	}
	t.Fatal("every gap came out identical; a fixed interval is as machine-like as none")
}

func TestSuccessiveSlotsAreSpacedByTheGap(t *testing.T) {
	g := newTestGate(30 * time.Second)
	now := time.Now()
	prev := g.due(now)
	for i := 0; i < 100; i++ {
		slot := g.due(now)
		if gap := slot.Sub(prev); gap < minGap {
			t.Fatalf("slots %d and %d are %s apart, below the %s floor", i, i+1, gap, minGap)
		}
		prev = slot
	}
}

func TestDisabledGateIsAlwaysDueNow(t *testing.T) {
	g := newTestGate(0)
	now := time.Now()
	for i := 0; i < 5; i++ {
		if due := g.due(now); due.After(now) {
			t.Fatalf("disabled gate held a send back by %s", due.Sub(now))
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
			t.Setenv("WHATSAPP_SEND_GAP_MEAN_SECONDS", tc.value)
			if got := envDurationSeconds("WHATSAPP_SEND_GAP_MEAN_SECONDS", 30*time.Second); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestEnvPositiveInt(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		want        int
	}{
		{"unset", "", 100},
		{"set", "7", 7},
		{"zero falls back", "0", 100},
		{"malformed falls back", "lots", 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("WHATSAPP_SEND_MAX_QUEUE_DEPTH", tc.value)
			if got := envPositiveInt("WHATSAPP_SEND_MAX_QUEUE_DEPTH", 100); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func meanSeconds(ds []time.Duration) float64 {
	var total float64
	for _, d := range ds {
		total += d.Seconds()
	}
	return total / float64(len(ds))
}
