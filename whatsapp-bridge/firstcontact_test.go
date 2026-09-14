package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

type fakeChecker struct {
	resp   []types.IsOnWhatsAppResponse
	err    error
	called [][]string
}

func (f *fakeChecker) IsOnWhatsApp(_ context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error) {
	f.called = append(f.called, phones)
	return f.resp, f.err
}

type fakeChats struct {
	known map[string]bool
	err   error
}

func (f fakeChats) GetChat(chatJID string, _ bool) (ChatView, bool, error) {
	if f.err != nil {
		return ChatView{}, false, f.err
	}
	if f.known[chatJID] {
		return ChatView{JID: chatJID}, true, nil
	}
	return ChatView{}, false, nil
}

func pnJID(user string) types.JID {
	return types.JID{User: user, Server: types.DefaultUserServer}
}

func TestVerifyRecipientRegisteredAllowsRegisteredNumber(t *testing.T) {
	checker := &fakeChecker{resp: []types.IsOnWhatsAppResponse{
		{Query: "+522225990829", IsIn: true},
	}}
	err := verifyRecipientRegistered(context.Background(), checker, fakeChats{}, pnJID("522225990829"))
	if err != nil {
		t.Fatalf("registered number rejected: %v", err)
	}
	if len(checker.called) != 1 || checker.called[0][0] != "522225990829" {
		t.Fatalf("queried %v, want one lookup of the bare number", checker.called)
	}
}

func TestVerifyRecipientRegisteredBlocksUnregisteredNumber(t *testing.T) {
	checker := &fakeChecker{resp: []types.IsOnWhatsAppResponse{
		{Query: "447578168548", IsIn: false},
	}}
	err := verifyRecipientRegistered(context.Background(), checker, fakeChats{}, pnJID("447578168548"))
	if err == nil {
		t.Fatal("unregistered number was allowed through")
	}
	if !strings.Contains(err.Error(), "not on WhatsApp") {
		t.Fatalf("unhelpful error for an unregistered number: %v", err)
	}
}

// The check exists to catch numbers we cannot confirm, so an empty or
// unmatched response must not read as permission to send.
func TestVerifyRecipientRegisteredFailsClosed(t *testing.T) {
	cases := []struct {
		name    string
		checker *fakeChecker
	}{
		{"lookup error", &fakeChecker{err: errors.New("websocket disconnected")}},
		{"empty response", &fakeChecker{resp: nil}},
		{"response for a different number", &fakeChecker{resp: []types.IsOnWhatsAppResponse{
			{Query: "447700900000", IsIn: true},
		}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := verifyRecipientRegistered(context.Background(), c.checker, fakeChats{}, pnJID("522225990829"))
			if err == nil {
				t.Fatal("send was allowed without a confirmed registration")
			}
		})
	}
}

// An established chat is the common path and must never pay for the lookup,
// nor be interruptible by it.
func TestVerifyRecipientRegisteredSkipsEstablishedChat(t *testing.T) {
	checker := &fakeChecker{err: errors.New("should not be called")}
	chats := fakeChats{known: map[string]bool{"447795894857@s.whatsapp.net": true}}
	if err := verifyRecipientRegistered(context.Background(), checker, chats, pnJID("447795894857")); err != nil {
		t.Fatalf("established chat blocked: %v", err)
	}
	if len(checker.called) != 0 {
		t.Fatalf("looked up an established contact: %v", checker.called)
	}
}

// A store failure must not start blocking sends to people we already talk to.
func TestVerifyRecipientRegisteredIgnoresStoreErrors(t *testing.T) {
	checker := &fakeChecker{err: errors.New("should not be called")}
	chats := fakeChats{err: errors.New("database is locked")}
	if err := verifyRecipientRegistered(context.Background(), checker, chats, pnJID("447795894857")); err != nil {
		t.Fatalf("store error turned into a send failure: %v", err)
	}
	if len(checker.called) != 0 {
		t.Fatalf("looked up despite an unreadable store: %v", checker.called)
	}
}

// Groups, broadcasts and LID-addressed chats have no phone number to check.
func TestVerifyRecipientRegisteredSkipsNonPhoneRecipients(t *testing.T) {
	checker := &fakeChecker{err: errors.New("should not be called")}
	for _, recipient := range []types.JID{
		{User: "120363000000000000", Server: types.GroupServer},
		{User: "111222333", Server: types.HiddenUserServer},
		{User: "", Server: types.DefaultUserServer},
		types.StatusBroadcastJID,
	} {
		if err := verifyRecipientRegistered(context.Background(), checker, fakeChats{}, recipient); err != nil {
			t.Fatalf("%s blocked: %v", recipient, err)
		}
	}
	if len(checker.called) != 0 {
		t.Fatalf("looked up a non-phone recipient: %v", checker.called)
	}
}

// WhatsApp may answer addressed by LID rather than by the number asked about.
func TestMatchesQueriedNumber(t *testing.T) {
	phone := "522225990829"
	cases := []struct {
		name string
		info types.IsOnWhatsAppResponse
		want bool
	}{
		{"bare query", types.IsOnWhatsAppResponse{Query: phone}, true},
		{"query with plus", types.IsOnWhatsAppResponse{Query: "+" + phone}, true},
		{"query with untrimmed server", types.IsOnWhatsAppResponse{Query: phone + "@c.us"}, true},
		{"matched on pn_jid", types.IsOnWhatsAppResponse{
			JID:         types.JID{User: "119546170081458", Server: types.HiddenUserServer},
			PhoneNumber: pnJID(phone),
		}, true},
		{"matched on jid", types.IsOnWhatsAppResponse{JID: pnJID(phone)}, true},
		{"different number", types.IsOnWhatsAppResponse{Query: "447700900000"}, false},
		{"lid only, no phone", types.IsOnWhatsAppResponse{
			JID: types.JID{User: "119546170081458", Server: types.HiddenUserServer},
		}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matchesQueriedNumber(c.info, phone); got != c.want {
				t.Fatalf("matchesQueriedNumber(%+v) = %v, want %v", c.info, got, c.want)
			}
		})
	}
}

// A nil checker is the no-session case and must not block anything.
func TestVerifyRecipientRegisteredWithoutChecker(t *testing.T) {
	if err := verifyRecipientRegistered(context.Background(), nil, fakeChats{}, pnJID("522225990829")); err != nil {
		t.Fatalf("nil checker blocked a send: %v", err)
	}
}
