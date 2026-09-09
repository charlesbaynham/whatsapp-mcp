package main

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func jid(user, server string) types.JID {
	return types.JID{User: user, Server: server}
}

// fakeAltResolver stands in for whatsmeow's local LID<->PN mapping store.
type fakeAltResolver struct {
	alts map[types.JID]types.JID
}

func (f fakeAltResolver) GetAltJID(_ context.Context, j types.JID) (types.JID, error) {
	return f.alts[j], nil
}

func TestResolveAlt(t *testing.T) {
	lid := jid("111", types.HiddenUserServer)
	pn := jid("447700900000", types.DefaultUserServer)
	resolver := fakeAltResolver{alts: map[types.JID]types.JID{lid: pn}}

	cases := []struct {
		name      string
		j         types.JID
		stanzaAlt types.JID
		resolver  altJIDResolver
		want      types.JID
	}{
		{"non-lid untouched", pn, types.JID{}, resolver, pn},
		{"stanza alt preferred", lid, jid("447700900111", types.DefaultUserServer), resolver, jid("447700900111", types.DefaultUserServer)},
		{"falls back to store", lid, types.JID{}, resolver, pn},
		{"unresolvable lid kept as-is", jid("999", types.HiddenUserServer), types.JID{}, resolver, jid("999", types.HiddenUserServer)},
		{"nil resolver kept as-is", lid, types.JID{}, nil, lid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := resolveAlt(context.Background(), c.resolver, c.j, c.stanzaAlt)
			if got != c.want {
				t.Fatalf("resolveAlt(%v, stanzaAlt=%v) = %v, want %v", c.j, c.stanzaAlt, got, c.want)
			}
		})
	}
}

func TestCanonicalChatJID(t *testing.T) {
	lidChat := jid("111", types.HiddenUserServer)
	pnFromStore := jid("447700900000", types.DefaultUserServer)
	pnFromStanza := jid("447700900111", types.DefaultUserServer)
	resolver := fakeAltResolver{alts: map[types.JID]types.JID{lidChat: pnFromStore}}
	group := jid("999", types.GroupServer)

	cases := []struct {
		name string
		info types.MessageSource
		want types.JID
	}{
		{
			name: "incoming DM under LID resolves via SenderAlt",
			info: types.MessageSource{Chat: lidChat, Sender: lidChat, SenderAlt: pnFromStanza},
			want: pnFromStanza,
		},
		{
			name: "incoming DM under LID falls back to store",
			info: types.MessageSource{Chat: lidChat, Sender: lidChat},
			want: pnFromStore,
		},
		{
			name: "outgoing DM to LID resolves via RecipientAlt",
			info: types.MessageSource{Chat: lidChat, IsFromMe: true, RecipientAlt: pnFromStanza},
			want: pnFromStanza,
		},
		{
			name: "group chat is never touched",
			info: types.MessageSource{Chat: group, IsGroup: true, Sender: lidChat, SenderAlt: pnFromStanza},
			want: group,
		},
		{
			name: "already a phone number is untouched",
			info: types.MessageSource{Chat: pnFromStanza, Sender: pnFromStanza},
			want: pnFromStanza,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := canonicalChatJID(context.Background(), resolver, c.info)
			if got != c.want {
				t.Fatalf("canonicalChatJID(%+v) = %v, want %v", c.info, got, c.want)
			}
		})
	}
}

func TestCanonicalSenderJID(t *testing.T) {
	myLID := jid("111", types.HiddenUserServer)
	myPN := jid("447700900000", types.DefaultUserServer)
	participantLID := jid("222", types.HiddenUserServer)
	participantPN := jid("447700900222", types.DefaultUserServer)
	peerPN := jid("447700900333", types.DefaultUserServer)
	resolver := fakeAltResolver{alts: map[types.JID]types.JID{myLID: myPN}}

	cases := []struct {
		name string
		info types.MessageSource
		chat types.JID
		want types.JID
	}{
		{
			name: "incoming DM shares the resolved chat identity",
			info: types.MessageSource{Sender: peerPN},
			chat: peerPN,
			want: peerPN,
		},
		{
			name: "outgoing DM resolves the local user's own LID",
			info: types.MessageSource{Sender: myLID, IsFromMe: true},
			chat: peerPN,
			want: myPN,
		},
		{
			name: "group participant resolves via SenderAlt",
			info: types.MessageSource{IsGroup: true, Sender: participantLID, SenderAlt: participantPN},
			chat: jid("999", types.GroupServer),
			want: participantPN,
		},
		{
			name: "group participant with no known mapping keeps LID",
			info: types.MessageSource{IsGroup: true, Sender: jid("333", types.HiddenUserServer)},
			chat: jid("999", types.GroupServer),
			want: jid("333", types.HiddenUserServer),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := canonicalSenderJID(context.Background(), resolver, c.info, c.chat)
			if got != c.want {
				t.Fatalf("canonicalSenderJID(%+v, chat=%v) = %v, want %v", c.info, c.chat, got, c.want)
			}
		})
	}
}
