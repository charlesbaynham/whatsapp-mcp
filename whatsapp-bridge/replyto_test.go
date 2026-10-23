package main

import (
	"context"
	"strings"
	"testing"
	"time"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func textReply(text, quotedID, participant string, quoted *waProto.Message) *waProto.Message {
	return &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
		Text: proto.String(text),
		ContextInfo: &waProto.ContextInfo{
			StanzaID:      proto.String(quotedID),
			Participant:   proto.String(participant),
			QuotedMessage: quoted,
		},
	}}
}

func TestExtractReplyToText(t *testing.T) {
	chat := jid("447700900001", types.DefaultUserServer)
	msg := textReply("yes please", "Q1", "447700900002@s.whatsapp.net",
		&waProto.Message{Conversation: proto.String("Do you want the bus?")})
	r := extractReplyTo(context.Background(), nil, msg, chat)
	if r == nil {
		t.Fatal("reply not detected")
	}
	want := ReplyRef{ID: "Q1", Sender: "447700900002", Content: "Do you want the bus?"}
	if *r != want {
		t.Fatalf("got %+v, want %+v", *r, want)
	}
}

func TestExtractReplyToResolvesLIDAndOtherChat(t *testing.T) {
	lid := jid("111", types.HiddenUserServer)
	pn := jid("447700900002", types.DefaultUserServer)
	resolver := fakeAltResolver{alts: map[types.JID]types.JID{lid: pn}}
	msg := textReply("about that", "Q2", "111@lid", &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("the map")}})
	msg.ExtendedTextMessage.ContextInfo.RemoteJID = proto.String("120363000000000000@g.us")
	r := extractReplyTo(context.Background(), resolver, msg, pn)
	if r == nil || r.Sender != "447700900002" || r.ChatJID != "120363000000000000@g.us" ||
		r.MediaType != "image" || r.Content != "the map" {
		t.Fatalf("got %+v", r)
	}
}

func TestExtractReplyToMediaAndWrapped(t *testing.T) {
	chat := jid("447700900001", types.DefaultUserServer)
	ci := &waProto.ContextInfo{StanzaID: proto.String("Q3"), QuotedMessage: &waProto.Message{Conversation: proto.String("send a photo")}}
	img := &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("here"), ContextInfo: ci}}
	if r := extractReplyTo(context.Background(), nil, img, chat); r == nil || r.ID != "Q3" {
		t.Fatalf("image reply: got %+v", r)
	}
	wrapped := &waProto.Message{EphemeralMessage: &waProto.FutureProofMessage{Message: img}}
	if r := extractReplyTo(context.Background(), nil, wrapped, chat); r == nil || r.ID != "Q3" {
		t.Fatalf("ephemeral reply: got %+v", r)
	}
}

func TestExtractReplyToIgnoresNonReplies(t *testing.T) {
	chat := jid("447700900001", types.DefaultUserServer)
	plain := &waProto.Message{Conversation: proto.String("hi")}
	// A mention carries ContextInfo but quotes nothing.
	mention := &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
		Text:        proto.String("@bot hi"),
		ContextInfo: &waProto.ContextInfo{MentionedJID: []string{"1@s.whatsapp.net"}},
	}}
	for _, m := range []*waProto.Message{plain, mention, nil} {
		if r := extractReplyTo(context.Background(), nil, m, chat); r != nil {
			t.Fatalf("non-reply detected as reply: %+v", r)
		}
	}
}

func TestReplyToStoredAndServed(t *testing.T) {
	store := newTestStore(t)
	chat := "447700900001@s.whatsapp.net"
	at := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(store.StoreChat(chat, "Guest", at))
	must(store.StoreMessage("q", chat, "447700900000", "Bus at 2:30?", at, true, "", "", "", nil, nil, nil, 0))
	must(store.StoreMessage("r", chat, "447700900001", "yes", at.Add(time.Minute), false, "", "", "", nil, nil, nil, 0))
	must(store.SetReplyTo("r", chat, &ReplyRef{ID: "q", Sender: "447700900000", Content: "Bus at 2:30?"}))

	msgs, err := store.ListMessages(ListMessagesParams{ChatJID: chat})
	must(err)
	if len(msgs) != 2 {
		t.Fatalf("got %d messages", len(msgs))
	}
	if msgs[1].ReplyTo != nil {
		t.Fatalf("original should not be a reply: %+v", msgs[1].ReplyTo)
	}
	r := msgs[0].ReplyTo
	if r == nil || r.ID != "q" || r.SenderName != "Me" || r.Content != "Bus at 2:30?" {
		t.Fatalf("reply_to = %+v", r)
	}

	ev, ok, err := store.messageEvent(chat, "r")
	must(err)
	if !ok || ev.ReplyTo == nil || ev.ReplyTo.ID != "q" {
		t.Fatalf("event reply_to = %+v", ev.ReplyTo)
	}
	ev.ChatName, ev.Timestamp = "Guest", at.Format(time.RFC3339)
	text := buildWebhookText([]WebhookEvent{ev})
	if !strings.Contains(text, `(replying to Me [q]: "Bus at 2:30?"): yes`) {
		t.Fatalf("webhook text lacks the reply:\n%s", text)
	}
}
