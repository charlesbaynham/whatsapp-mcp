package main

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type fakeSaltStore struct {
	salt      []byte
	err       error
	afterSync []byte
}

func (f *fakeSaltStore) GetNCTSalt(context.Context) ([]byte, error) {
	return f.salt, f.err
}

type fakeAppStateFetcher struct {
	store *fakeSaltStore
	err   error
	calls []struct {
		name     appstate.WAPatchName
		fullSync bool
	}
}

func (f *fakeAppStateFetcher) FetchAppState(_ context.Context, name appstate.WAPatchName, fullSync, _ bool) error {
	f.calls = append(f.calls, struct {
		name     appstate.WAPatchName
		fullSync bool
	}{name, fullSync})
	if f.err != nil {
		return f.err
	}
	if f.store != nil {
		f.store.salt = f.store.afterSync
	}
	return nil
}

var noopLog = waLog.Noop

func TestEnsureNCTSaltSkipsFetchWhenSaltPresent(t *testing.T) {
	store := &fakeSaltStore{salt: []byte("salt")}
	fetcher := &fakeAppStateFetcher{}
	if !ensureNCTSalt(context.Background(), store, fetcher, noopLog) {
		t.Fatal("stored salt reported as missing")
	}
	if len(fetcher.calls) != 0 {
		t.Fatalf("fetched app state despite a stored salt: %v", fetcher.calls)
	}
}

func TestEnsureNCTSaltFetchesRegularHighInFull(t *testing.T) {
	store := &fakeSaltStore{afterSync: []byte("salt")}
	fetcher := &fakeAppStateFetcher{store: store}
	if !ensureNCTSalt(context.Background(), store, fetcher, noopLog) {
		t.Fatal("salt delivered by the re-sync was not reported")
	}
	if len(fetcher.calls) != 1 || fetcher.calls[0].name != appstate.WAPatchRegularHigh || !fetcher.calls[0].fullSync {
		t.Fatalf("calls = %+v, want one full sync of regular_high", fetcher.calls)
	}
}

func TestEnsureNCTSaltReportsMissingWhenServerHasNone(t *testing.T) {
	store := &fakeSaltStore{}
	fetcher := &fakeAppStateFetcher{store: store}
	if ensureNCTSalt(context.Background(), store, fetcher, noopLog) {
		t.Fatal("reported a salt the server never sent")
	}
	if len(fetcher.calls) != 1 {
		t.Fatalf("calls = %+v, want exactly one attempt", fetcher.calls)
	}
}

func TestEnsureNCTSaltFailsQuietly(t *testing.T) {
	cases := []struct {
		name    string
		store   *fakeSaltStore
		fetcher *fakeAppStateFetcher
		fetches int
	}{
		{"store error", &fakeSaltStore{err: errors.New("database is locked")}, &fakeAppStateFetcher{}, 0},
		{"fetch error", &fakeSaltStore{}, &fakeAppStateFetcher{err: errors.New("websocket disconnected")}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if ensureNCTSalt(context.Background(), c.store, c.fetcher, noopLog) {
				t.Fatal("reported a salt on a failure path")
			}
			if len(c.fetcher.calls) != c.fetches {
				t.Fatalf("calls = %+v, want %d", c.fetcher.calls, c.fetches)
			}
		})
	}
}

func TestHasNCTSaltWithoutStore(t *testing.T) {
	present, err := hasNCTSalt(context.Background(), nil)
	if err != nil || present {
		t.Fatalf("nil store: present=%v err=%v, want false, nil", present, err)
	}
}
