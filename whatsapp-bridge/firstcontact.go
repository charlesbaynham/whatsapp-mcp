package main

import (
	"context"
	"fmt"
	"strings"

	"go.mau.fi/whatsmeow/types"
)

// registrationChecker asks WhatsApp whether a phone number is registered. It
// is the one method of *whatsmeow.Client this check needs, named separately so
// the logic is testable without a WhatsApp session.
type registrationChecker interface {
	IsOnWhatsApp(ctx context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error)
}

// chatExistenceChecker reports whether the bridge already holds a chat with a
// JID. *MessageStore implements it via GetChat.
type chatExistenceChecker interface {
	GetChat(chatJID string, includeLastMessage bool) (ChatView, bool, error)
}

// isFirstContact reports whether this bridge has never had a chat with the
// recipient. A store error is treated as "not first contact": the check below
// exists to catch unreachable numbers, and a database hiccup must not start
// blocking sends to people we are already talking to.
func isFirstContact(chats chatExistenceChecker, recipient types.JID) bool {
	if chats == nil {
		return false
	}
	_, found, err := chats.GetChat(recipient.ToNonAD().String(), false)
	if err != nil {
		return false
	}
	return !found
}

// needsRegistrationCheck reports whether a recipient is the kind of JID worth
// asking WhatsApp about. Only ordinary phone-number recipients qualify: a
// group, a broadcast list, a newsletter or a LID-addressed chat either has no
// phone number to look up or could only have been learned from WhatsApp in the
// first place.
func needsRegistrationCheck(recipient types.JID) bool {
	return recipient.Server == types.DefaultUserServer && recipient.User != ""
}

// verifyRecipientRegistered checks, before the first ever message to a
// recipient, that the number is actually on WhatsApp, and returns a non-nil
// error if it is not.
//
// The point is to fail *before* sending rather than after. A message addressed
// to a number that is not on WhatsApp cannot be delivered, but the attempt is
// still counted by WhatsApp as reaching out to an unknown contact, which is
// what accrues its "reach-out time-lock" rate limit (see
// https://github.com/WhiskeySockets/Baileys/issues/2441). A list with a few
// bad numbers in it therefore costs far more than the failed sends themselves.
//
// It is deliberately limited to first contact. Every later message in an
// established chat skips the lookup, so the common path pays nothing and an
// ongoing conversation can never be interrupted by this check.
//
// ⚠️ It fails closed: a lookup that errors, or comes back with no entry for
// the number, blocks the send. "We could not confirm this number exists" is
// the case the check is for, so treating it as permission to reach out anyway
// would defeat it. The caller sees a distinct message and can retry.
//
// A successful check is not wasted work: IsOnWhatsApp caches the PN->LID
// mapping it gets back — but under WhatsApp's own canonical spelling of the
// phone number, not necessarily the one queried (a Mexican number stored as
// 52+10 digits is cached as 521+10). The canonical JID is therefore returned
// so the caller can send to the number the cache actually holds.
func verifyRecipientRegistered(ctx context.Context, checker registrationChecker, chats chatExistenceChecker, recipient types.JID) (types.JID, error) {
	if checker == nil || !needsRegistrationCheck(recipient) || !isFirstContact(chats, recipient) {
		return recipient, nil
	}

	phone := recipient.User
	resp, err := checker.IsOnWhatsApp(ctx, []string{phone})
	if err != nil {
		return recipient, fmt.Errorf("could not check whether %s is on WhatsApp: %w", phone, err)
	}

	for _, info := range resp {
		if !matchesQueriedNumber(info, phone) {
			continue
		}
		if !info.IsIn {
			return recipient, fmt.Errorf("%s is not on WhatsApp", phone)
		}
		return canonicalRecipient(info, recipient), nil
	}
	return recipient, fmt.Errorf("WhatsApp returned no registration status for %s", phone)
}

// canonicalRecipient picks the phone-number JID to actually send to out of a
// matched usync entry, preferring the dedicated pn_jid field and falling back
// to JID itself when that is already a phone number. A LID-only entry (no
// phone-number field populated) carries nothing to rewrite to, so the original
// recipient stands.
func canonicalRecipient(info types.IsOnWhatsAppResponse, fallback types.JID) types.JID {
	if info.PhoneNumber.Server == types.DefaultUserServer && info.PhoneNumber.User != "" {
		return info.PhoneNumber
	}
	if info.JID.Server == types.DefaultUserServer && info.JID.User != "" {
		return info.JID
	}
	return fallback
}

// matchesQueriedNumber reports whether a usync response entry is the answer to
// the number we asked about. WhatsApp may address the entry by LID rather than
// by phone number, so the phone number is looked for in every field that can
// carry it rather than only in JID.
func matchesQueriedNumber(info types.IsOnWhatsAppResponse, phone string) bool {
	if normalisePhone(info.Query) == phone {
		return true
	}
	if info.PhoneNumber.User == phone {
		return true
	}
	return info.JID.Server == types.DefaultUserServer && info.JID.User == phone
}

// normalisePhone strips the decoration a usync query string can carry — a
// leading "+", and a server suffix whatsmeow did not trim — leaving the bare
// digits the rest of the bridge addresses people by.
func normalisePhone(s string) string {
	if at := strings.IndexByte(s, '@'); at >= 0 {
		s = s[:at]
	}
	return strings.TrimPrefix(s, "+")
}
