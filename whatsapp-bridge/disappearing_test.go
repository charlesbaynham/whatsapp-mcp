package main

import (
	"context"
	"testing"
	"time"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

const week = 7 * 24 * 3600

func TestDisappearingFromMessage(t *testing.T) {
	set := &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
		Type:                      waProto.ProtocolMessage_EPHEMERAL_SETTING.Enum(),
		EphemeralExpiration:       proto.Uint32(0),
		EphemeralSettingTimestamp: proto.Int64(1700000000),
	}}
	if got, ok := disappearingFromMessage(set); !ok || got.Expiration != 0 || got.SettingTimestamp != 1700000000 {
		t.Fatalf("setting change: got %+v %v", got, ok)
	}

	text := &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
		Text:        proto.String("hi"),
		ContextInfo: &waProto.ContextInfo{Expiration: proto.Uint32(week), EphemeralSettingTimestamp: proto.Int64(1700000001)},
	}}
	if got, ok := disappearingFromMessage(text); !ok || got.Expiration != week || got.SettingTimestamp != 1700000001 {
		t.Fatalf("text: got %+v %v", got, ok)
	}

	wrapped := &waProto.Message{EphemeralMessage: &waProto.FutureProofMessage{Message: text}}
	if got, ok := disappearingFromMessage(wrapped); !ok || got.Expiration != week {
		t.Fatalf("wrapped: got %+v %v", got, ok)
	}

	// A plain message says nothing about the timer, so it must not read as "off".
	if _, ok := disappearingFromMessage(&waProto.Message{Conversation: proto.String("hi")}); ok {
		t.Fatal("plain conversation reported a timer")
	}
}

func TestApplyDisappearingTimer(t *testing.T) {
	timer := DisappearingTimer{Expiration: week, SettingTimestamp: 1700000000}

	msg := &waProto.Message{Conversation: proto.String("hello")}
	applyDisappearingTimer(msg, timer)
	if msg.Conversation != nil || msg.GetExtendedTextMessage().GetText() != "hello" {
		t.Fatalf("text not converted: %v", msg)
	}
	if ci := msg.GetExtendedTextMessage().GetContextInfo(); ci.GetExpiration() != week || ci.GetEphemeralSettingTimestamp() != 1700000000 {
		t.Fatalf("text context info: %v", ci)
	}

	img := &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("x")}}
	applyDisappearingTimer(img, timer)
	if img.GetImageMessage().GetContextInfo().GetExpiration() != week {
		t.Fatal("image not stamped")
	}

	poll := &waProto.Message{PollCreationMessageV3: &waProto.PollCreationMessage{Name: proto.String("q")}}
	applyDisappearingTimer(poll, timer)
	if poll.GetPollCreationMessageV3().GetContextInfo().GetExpiration() != week {
		t.Fatal("poll not stamped")
	}

	off := &waProto.Message{Conversation: proto.String("hello")}
	applyDisappearingTimer(off, DisappearingTimer{})
	if off.GetConversation() != "hello" || off.ExtendedTextMessage != nil {
		t.Fatal("timer off changed the message")
	}
}

func TestDisappearingStoreKeepsNewest(t *testing.T) {
	store := newTestStore(t)
	chat := "447700900001@s.whatsapp.net"
	if _, known, err := store.GetDisappearingTimer(chat); err != nil || known {
		t.Fatalf("fresh chat: known=%v err=%v", known, err)
	}
	now := time.Now()
	must(t, store.SetDisappearingTimer(chat, DisappearingTimer{Expiration: week}, now))
	// An older observation (e.g. history sync) must not overwrite it.
	must(t, store.SetDisappearingTimer(chat, DisappearingTimer{Expiration: 0}, now.Add(-time.Hour)))
	if got, _, _ := store.GetDisappearingTimer(chat); got.Expiration != week {
		t.Fatalf("older observation won: %+v", got)
	}
	must(t, store.SetDisappearingTimer(chat, DisappearingTimer{Expiration: 0}, now.Add(time.Hour)))
	if got, known, _ := store.GetDisappearingTimer(chat); !known || got.Expiration != 0 {
		t.Fatalf("turning off not recorded: %+v", got)
	}
}

type fakeGroupInfo struct {
	calls int
	info  types.GroupInfo
}

func (f *fakeGroupInfo) GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error) {
	f.calls++
	return &f.info, nil
}

func TestDisappearingTimerForSendFetchesGroupOnce(t *testing.T) {
	store := newTestStore(t)
	group := jid("120363000000000000", types.GroupServer)
	f := &fakeGroupInfo{info: types.GroupInfo{GroupEphemeral: types.GroupEphemeral{IsEphemeral: true, DisappearingTimer: 86400}}}
	for i := 0; i < 2; i++ {
		got, err := disappearingTimerForSend(context.Background(), store, f, group)
		if err != nil || got.Expiration != 86400 {
			t.Fatalf("got %+v %v", got, err)
		}
	}
	if f.calls != 1 {
		t.Fatalf("group info fetched %d times", f.calls)
	}
	// A live change supersedes the fetched value.
	must(t, store.SetDisappearingTimer(group.String(), DisappearingTimer{}, time.Now()))
	if got, _ := disappearingTimerForSend(context.Background(), store, f, group); got.Expiration != 0 {
		t.Fatalf("live change ignored: %+v", got)
	}

	// DMs are never looked up.
	dm := jid("447700900001", types.DefaultUserServer)
	if got, err := disappearingTimerForSend(context.Background(), store, f, dm); err != nil || got.Expiration != 0 || f.calls != 1 {
		t.Fatalf("dm: %+v %v calls=%d", got, err, f.calls)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
