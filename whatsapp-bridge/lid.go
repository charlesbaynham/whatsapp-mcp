package main

import (
	"context"

	"go.mau.fi/whatsmeow/types"
)

// altJIDResolver looks up the other addressing form (phone number <-> LID)
// of a JID from whatsmeow's local mapping store. It is purely local: the
// mapping was either parsed synchronously off the stanza of the message
// being handled, or learned and cached from an earlier one — there is no
// WhatsApp server round trip involved. *whatsmeow.Client's Store field
// implements this directly.
type altJIDResolver interface {
	GetAltJID(ctx context.Context, jid types.JID) (types.JID, error)
}

// resolveAlt returns the phone-number counterpart of a LID JID, preferring
// stanzaAlt (whatsmeow's parse of the current message's own addressing
// attributes) and falling back to the resolver's cached mapping. Anything
// that isn't a LID, or that neither source can resolve, is returned as-is.
func resolveAlt(ctx context.Context, resolver altJIDResolver, jid, stanzaAlt types.JID) types.JID {
	if jid.Server != types.HiddenUserServer {
		return jid
	}
	if !stanzaAlt.IsEmpty() {
		return stanzaAlt
	}
	if resolver == nil {
		return jid
	}
	if alt, err := resolver.GetAltJID(ctx, jid); err == nil && !alt.IsEmpty() {
		return alt
	}
	return jid
}

// canonicalChatJID returns the JID that should identify a chat in the
// database. WhatsApp's LID rollout can address a 1:1 chat by an opaque
// hidden identifier instead of the contact's phone number; resolving it
// here keeps one chat row per contact, findable by phone number, instead of
// splitting a contact across both addressing forms.
func canonicalChatJID(ctx context.Context, resolver altJIDResolver, info types.MessageSource) types.JID {
	alt := info.SenderAlt
	if info.IsFromMe {
		alt = info.RecipientAlt
	}
	return resolveAlt(ctx, resolver, info.Chat, alt)
}

// canonicalSenderJID returns the JID to record as a message's sender. In a
// 1:1 chat the sender is always either the local user or the other party,
// so a message not from the local user shares the chat's already-resolved
// identity; one from the local user resolves the local user's own LID
// (whatsmeow learns that mapping locally as soon as it connects). In a
// group, the sender is a participant resolved independently of the chat.
func canonicalSenderJID(ctx context.Context, resolver altJIDResolver, info types.MessageSource, chat types.JID) types.JID {
	switch {
	case info.IsGroup:
		return resolveAlt(ctx, resolver, info.Sender, info.SenderAlt)
	case info.IsFromMe:
		return resolveAlt(ctx, resolver, info.Sender, types.JID{})
	default:
		return chat
	}
}

// canonicalHistorySyncChatJID resolves a history-sync Conversation's own JID
// the same way canonicalChatJID resolves a live message's chat. A live
// stanza's SenderAlt/RecipientAlt has no equivalent on the per-message
// records history sync carries, but the Conversation record itself carries
// one for its own JID: pnJID, the phone-number counterpart of a
// LID-addressed conversation. Treat it exactly as a stanza alt, preferred
// over the resolver's local cache. pnJID that fails to parse is treated as
// absent rather than as an error, since the local cache is still a valid
// fallback.
func canonicalHistorySyncChatJID(ctx context.Context, resolver altJIDResolver, jid types.JID, pnJID string) types.JID {
	// types.ParseJID barely validates: a bare string with no "@" parses
	// "successfully" into a JID with an empty User, so a missing or
	// malformed pnJID must be caught on User rather than on err alone.
	alt := types.JID{}
	if parsed, err := types.ParseJID(pnJID); err == nil && parsed.User != "" {
		alt = parsed
	}
	return resolveAlt(ctx, resolver, jid, alt)
}
