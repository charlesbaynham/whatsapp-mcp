package main

// Replies: when someone swipes to reply, WhatsApp attaches a ContextInfo to
// the new message naming the quoted message's id and sender, and carrying a
// copy of the quoted message itself. That is the only record of the link,
// so it is captured at ingest and served as `reply_to` on every message.

import (
	"context"
	"database/sql"
	"fmt"

	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ReplyRef identifies the message a message replies to.
type ReplyRef struct {
	// ID is the quoted message's id: pass it to get_message_context.
	ID string `json:"id"`
	// Sender is the quoted message's author, stored like messages.sender.
	Sender string `json:"sender,omitempty"`
	// SenderName is the display name for Sender, or "Me" for this account.
	SenderName string `json:"sender_name,omitempty"`
	// Content is the quoted text (or caption, or poll question) as WhatsApp
	// copied it into the reply, so it is there even when the original is not
	// in the store. May be empty for quoted media without a caption.
	Content   string `json:"content,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	// ChatJID is set only when the quoted message lives in another chat
	// ("reply privately" to a group message).
	ChatJID string `json:"chat_jid,omitempty"`
}

var replyColumns = []string{"reply_to_id", "reply_to_sender", "reply_to_content", "reply_to_media_type", "reply_to_chat_jid"}

func ensureReplyColumns(db *sql.DB) error {
	for _, col := range replyColumns {
		if err := ensureColumn(db, "messages", col, "TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("failed to add messages.%s: %v", col, err)
		}
	}
	return nil
}

// SetReplyTo records what a stored message replies to. nil is a no-op.
func (store *MessageStore) SetReplyTo(id, chatJID string, r *ReplyRef) error {
	if r == nil {
		return nil
	}
	_, err := store.db.Exec(
		`UPDATE messages SET reply_to_id = ?, reply_to_sender = ?, reply_to_content = ?, reply_to_media_type = ?, reply_to_chat_jid = ?
		 WHERE id = ? AND chat_jid = ?`,
		r.ID, r.Sender, r.Content, r.MediaType, r.ChatJID, id, chatJID)
	return err
}

type contextInfoHolder interface {
	GetContextInfo() *waProto.ContextInfo
}

type wrappedMessage interface {
	GetMessage() *waProto.Message
}

// findContextInfo returns the ContextInfo of whichever content field msg
// carries (extended text, image, video, document, sticker, ...), looking
// through wrappers such as ephemeral or view-once messages.
func findContextInfo(msg *waProto.Message) *waProto.ContextInfo {
	return findContextInfoDepth(msg, 0)
}

func findContextInfoDepth(msg *waProto.Message, depth int) *waProto.ContextInfo {
	if msg == nil || depth > 3 {
		return nil
	}
	var found *waProto.ContextInfo
	msg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
			return true
		}
		inner := v.Message().Interface()
		if h, ok := inner.(contextInfoHolder); ok {
			if ci := h.GetContextInfo(); ci.GetStanzaID() != "" {
				found = ci
				return false
			}
		}
		if w, ok := inner.(wrappedMessage); ok {
			if ci := findContextInfoDepth(w.GetMessage(), depth+1); ci != nil {
				found = ci
				return false
			}
		}
		return true
	})
	return found
}

// quotedPreview is the text and media type of a quoted message copy.
func quotedPreview(q *waProto.Message) (content, mediaType string) {
	if q == nil {
		return "", ""
	}
	if p := extractPoll(q); p != nil {
		return p.Question, mediaTypePoll
	}
	content = extractTextContent(q)
	mediaType, _, _, _, _, _, _ = extractMediaInfo(q)
	switch {
	case q.GetImageMessage() != nil:
		content = q.GetImageMessage().GetCaption()
	case q.GetVideoMessage() != nil:
		content = q.GetVideoMessage().GetCaption()
	case q.GetDocumentMessage() != nil:
		content = q.GetDocumentMessage().GetCaption()
	case q.GetStickerMessage() != nil:
		mediaType = "sticker"
	case q.GetLocationMessage() != nil:
		mediaType = "location"
		content = q.GetLocationMessage().GetName()
	case q.GetContactMessage() != nil:
		mediaType = "contact"
		content = q.GetContactMessage().GetDisplayName()
	}
	return content, mediaType
}

// extractReplyTo returns what msg replies to, or nil if it isn't a reply.
// LID-addressed senders and chats are resolved to phone numbers where the
// local mapping store knows them, as for the message's own sender.
func extractReplyTo(ctx context.Context, resolver altJIDResolver, msg *waProto.Message, chat types.JID) *ReplyRef {
	ci := findContextInfo(msg)
	if ci == nil {
		return nil
	}
	r := &ReplyRef{ID: ci.GetStanzaID()}
	if p := ci.GetParticipant(); p != "" {
		if jid, err := types.ParseJID(p); err == nil && jid.User != "" {
			r.Sender = resolveAlt(ctx, resolver, jid, types.JID{}).User
		} else {
			r.Sender = p
		}
	}
	if remote := ci.GetRemoteJID(); remote != "" {
		if jid, err := types.ParseJID(remote); err == nil {
			jid = resolveAlt(ctx, resolver, jid, types.JID{})
			if jid.String() != chat.String() {
				r.ChatJID = jid.String()
			}
		}
	}
	r.Content, r.MediaType = quotedPreview(ci.GetQuotedMessage())
	return r
}

// replyPreviewLen caps the quoted text shown inline in a webhook's text.
const replyPreviewLen = 120

// describeReplyTo is the inline form used in webhook text, e.g.
// `(replying to Me [3EB0…]: "What time is the bus?")`.
func describeReplyTo(r *ReplyRef) string {
	who := r.SenderName
	if who == "" {
		who = r.Sender
	}
	if who == "" {
		who = "unknown"
	}
	quoted := r.Content
	if runes := []rune(quoted); len(runes) > replyPreviewLen {
		quoted = string(runes[:replyPreviewLen]) + "…"
	}
	switch {
	case r.MediaType != "" && quoted != "":
		quoted = fmt.Sprintf("[%s] %q", r.MediaType, quoted)
	case r.MediaType != "":
		quoted = "[" + r.MediaType + "]"
	default:
		quoted = fmt.Sprintf("%q", quoted)
	}
	where := ""
	if r.ChatJID != "" {
		where = " in " + r.ChatJID
	}
	return fmt.Sprintf("(replying to %s [%s]%s: %s)", who, r.ID, where, quoted)
}

// replyRefFor reads back what a stored message replies to, or nil.
func (store *MessageStore) replyRefFor(chatJID, messageID string) *ReplyRef {
	var r ReplyRef
	err := store.db.QueryRow(
		`SELECT reply_to_id, reply_to_sender, reply_to_content, reply_to_media_type, reply_to_chat_jid
		 FROM messages WHERE chat_jid = ? AND id = ?`, chatJID, messageID,
	).Scan(&r.ID, &r.Sender, &r.Content, &r.MediaType, &r.ChatJID)
	if err != nil || r.ID == "" {
		return nil
	}
	return store.nameReply(&r)
}
