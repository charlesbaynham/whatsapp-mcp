package main

import (
	"context"

	"go.mau.fi/whatsmeow/appstate"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// nctSaltStore is the one method of store.NCTSaltStore this needs.
type nctSaltStore interface {
	GetNCTSalt(ctx context.Context) ([]byte, error)
}

// appStateFetcher is *whatsmeow.Client's FetchAppState.
type appStateFetcher interface {
	FetchAppState(ctx context.Context, name appstate.WAPatchName, fullSync, onlyIfNotSynced bool) error
}

func hasNCTSalt(ctx context.Context, salts nctSaltStore) (bool, error) {
	if salts == nil {
		return false, nil
	}
	salt, err := salts.GetNCTSalt(ctx)
	if err != nil {
		return false, err
	}
	return len(salt) > 0, nil
}

// ensureNCTSalt re-syncs the regular_high app-state collection in full when no
// NCT salt is stored, and reports whether one is held afterwards.
//
// The salt matters for first contact: the cstoken a first message carries is
// HMAC(salt, recipient LID), and a message with no privacy token at all is
// what WhatsApp counts as "reaching out" and rate-limits with error 463. The
// salt arrives as a regular_high mutation, but after the initial sync whatsmeow
// only pulls that collection incrementally, so a device that never received
// one never asks again. Fetching the collection in full is how the official
// clients recover it; if the server holds no salt for the account this is a
// small no-op.
func ensureNCTSalt(ctx context.Context, salts nctSaltStore, fetcher appStateFetcher, logger waLog.Logger) bool {
	present, err := hasNCTSalt(ctx, salts)
	if err != nil {
		logger.Warnf("Failed to read NCT salt: %v", err)
		return false
	}
	if present {
		return true
	}
	logger.Infof("No NCT salt stored; re-syncing %s app state in full", appstate.WAPatchRegularHigh)
	if err := fetcher.FetchAppState(ctx, appstate.WAPatchRegularHigh, true, false); err != nil {
		logger.Warnf("Failed to re-sync %s app state: %v", appstate.WAPatchRegularHigh, err)
		return false
	}
	present, err = hasNCTSalt(ctx, salts)
	if err != nil {
		logger.Warnf("Failed to read NCT salt after re-sync: %v", err)
		return false
	}
	if present {
		logger.Infof("NCT salt received: first-contact sends can now carry a cstoken")
	} else {
		logger.Warnf("Still no NCT salt after a full %s sync: WhatsApp has not provisioned one for this account, so first-contact sends go out with no privacy token", appstate.WAPatchRegularHigh)
	}
	return present
}
