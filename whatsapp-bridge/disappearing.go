package main

// Disappearing messages: WhatsApp keeps a chat's timer on each device, and
// every message sent into a disappearing chat names that timer in its
// ContextInfo.Expiration. A message without it lands in a disappearing chat
// as "there was a conflict with the disappearing messages timer" and never
// disappears. So the bridge records the timer for each chat as it learns it,
// and stamps it on every message it sends.
//
// Where the timer is learnt from:
//   - an EPHEMERAL_SETTING protocol message (someone changed it in a DM);
//   - the Expiration on any incoming message's ContextInfo;
//   - group info, as a change notification or a fetch;
//   - history sync, which carries each conversation's setting.
// Each source carries a time; an older one never overwrites a newer one.

import (
	"context"
	"database/sql"
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	waProto "go.mau.fi/whatsmeow/binary/proto"
)

// DisappearingTimer is a chat's disappearing-messages setting.
type DisappearingTimer struct {
	// Expiration in seconds; 0 means off.
	Expiration uint32
	// SettingTimestamp is when the timer was set, if WhatsApp said (unix s).
	SettingTimestamp int64
}

func createDisappearingTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS chat_disappearing (
			chat_jid TEXT PRIMARY KEY,
			expiration INTEGER NOT NULL,
			setting_timestamp INTEGER NOT NULL DEFAULT 0,
			observed_at INTEGER NOT NULL
		);
	`)
	return err
}

// SetDisappearingTimer records chat's timer as seen at observedAt, unless a
// later observation is already stored.
func (store *MessageStore) SetDisappearingTimer(chatJID string, t DisappearingTimer, observedAt time.Time) error {
	_, err := store.db.Exec(`
		INSERT INTO chat_disappearing (chat_jid, expiration, setting_timestamp, observed_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(chat_jid) DO UPDATE SET
			expiration = excluded.expiration,
			setting_timestamp = excluded.setting_timestamp,
			observed_at = excluded.observed_at
		WHERE excluded.observed_at >= chat_disappearing.observed_at`,
		chatJID, t.Expiration, t.SettingTimestamp, observedAt.Unix())
	return err
}

// GetDisappearingTimer returns chat's recorded timer; known is false when the
// bridge has never learnt it.
func (store *MessageStore) GetDisappearingTimer(chatJID string) (t DisappearingTimer, known bool, err error) {
	err = store.db.QueryRow(
		`SELECT expiration, setting_timestamp FROM chat_disappearing WHERE chat_jid = ?`, chatJID,
	).Scan(&t.Expiration, &t.SettingTimestamp)
	if err == sql.ErrNoRows {
		return DisappearingTimer{}, false, nil
	}
	return t, err == nil, err
}

// disappearingFromMessage returns the timer a message announces, if any.
// An explicit EPHEMERAL_SETTING wins, and is the only way to learn "off": a
// content message without an Expiration may just come from a client that
// left the ContextInfo out, so it says nothing.
func disappearingFromMessage(msg *waProto.Message) (DisappearingTimer, bool) {
	if pm := msg.GetProtocolMessage(); pm != nil && pm.GetType() == waProto.ProtocolMessage_EPHEMERAL_SETTING {
		return DisappearingTimer{Expiration: pm.GetEphemeralExpiration(), SettingTimestamp: pm.GetEphemeralSettingTimestamp()}, true
	}
	if ci := findContextInfoWith(msg, func(ci *waProto.ContextInfo) bool { return ci.GetExpiration() > 0 }); ci != nil {
		return DisappearingTimer{Expiration: ci.GetExpiration(), SettingTimestamp: ci.GetEphemeralSettingTimestamp()}, true
	}
	return DisappearingTimer{}, false
}

// noteDisappearingFromMessage records the timer an incoming message carries.
func noteDisappearingFromMessage(store *MessageStore, chatJID string, msg *events.Message) error {
	t, ok := disappearingFromMessage(msg.Message)
	if !ok {
		return nil
	}
	return store.SetDisappearingTimer(chatJID, t, msg.Info.Timestamp)
}

func timerFromGroup(e types.GroupEphemeral) DisappearingTimer {
	if !e.IsEphemeral {
		return DisappearingTimer{}
	}
	return DisappearingTimer{Expiration: e.DisappearingTimer}
}

// groupInfoFetcher is the slice of *whatsmeow.Client used to look a group's
// timer up the first time the bridge sends there.
type groupInfoFetcher interface {
	GetGroupInfo(ctx context.Context, jid types.JID) (*types.GroupInfo, error)
}

// disappearingTimerForSend returns the timer to stamp on a message to chat.
// A group the bridge has never seen a setting for is looked up once and
// cached. A DM can't be looked up: until its first incoming message or
// setting change, it is assumed off.
func disappearingTimerForSend(ctx context.Context, store *MessageStore, groups groupInfoFetcher, chat types.JID) (DisappearingTimer, error) {
	t, known, err := store.GetDisappearingTimer(chat.String())
	if err != nil || known || chat.Server != types.GroupServer || groups == nil {
		return t, err
	}
	info, err := groups.GetGroupInfo(ctx, chat)
	if err != nil {
		return DisappearingTimer{}, err
	}
	t = timerFromGroup(info.GroupEphemeral)
	// Observed "at the epoch", so any real event later supersedes it.
	return t, store.SetDisappearingTimer(chat.String(), t, time.Unix(0, 0))
}

// applyDisappearingTimer stamps t on msg's content, converting a bare
// Conversation into an ExtendedTextMessage, which can carry a ContextInfo.
// A zero timer leaves msg untouched.
func applyDisappearingTimer(msg *waProto.Message, t DisappearingTimer) {
	if msg == nil || t.Expiration == 0 {
		return
	}
	if msg.Conversation != nil {
		msg.ExtendedTextMessage = &waProto.ExtendedTextMessage{Text: msg.Conversation}
		msg.Conversation = nil
	}
	ci := mutableContextInfo(msg)
	if ci == nil {
		return
	}
	ci.Expiration = proto.Uint32(t.Expiration)
	if t.SettingTimestamp != 0 {
		ci.EphemeralSettingTimestamp = proto.Int64(t.SettingTimestamp)
	}
}

var contextInfoName = (&waProto.ContextInfo{}).ProtoReflect().Descriptor().FullName()

// mutableContextInfo returns the ContextInfo of whichever content field msg
// carries (creating it if absent), or nil if none of them has one.
func mutableContextInfo(msg *waProto.Message) *waProto.ContextInfo {
	var found *waProto.ContextInfo
	msg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
			return true
		}
		inner := v.Message()
		fields := inner.Descriptor().Fields()
		for i := 0; i < fields.Len(); i++ {
			f := fields.Get(i)
			if f.Kind() == protoreflect.MessageKind && !f.IsList() && f.Message().FullName() == contextInfoName {
				found = inner.Mutable(f).Message().Interface().(*waProto.ContextInfo)
				return false
			}
		}
		return true
	})
	return found
}
