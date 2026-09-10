package main

import (
	"context"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// MarkReadRequest is the body of POST /api/mark-read.
type MarkReadRequest struct {
	ChatJID     string `json:"chat_jid"`
	SendReceipt bool   `json:"send_receipt"`
}

// MarkReadResponse is the response body of POST /api/mark-read.
type MarkReadResponse struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	MarkedCount int    `json:"marked_count"`
	ReceiptSent bool   `json:"receipt_sent"`
}

// MarkRead takes one sender per call, so unread messages must be grouped by sender first.
func groupUnreadBySender(unread []UnreadMessage) (order []string, bySender map[string][]types.MessageID) {
	bySender = make(map[string][]types.MessageID)
	for _, u := range unread {
		if _, seen := bySender[u.Sender]; !seen {
			order = append(order, u.Sender)
		}
		bySender[u.Sender] = append(bySender[u.Sender], types.MessageID(u.ID))
	}
	return order, bySender
}

// Sender is stored as a bare user; 1:1 sender is the chat itself, group senders are rebuilt
// as phone-number JIDs, which is wrong only for a LID that was never resolved at receive time.
func senderJIDForMarkRead(chat types.JID, senderUser string) types.JID {
	if chat.Server != types.GroupServer {
		return chat
	}
	return types.JID{User: senderUser, Server: types.DefaultUserServer}
}

// Only reacts to our own devices reading a chat, never to receipts about our outgoing messages.
func handleReceipt(client *whatsmeow.Client, messageStore *MessageStore, v *events.Receipt, logger waLog.Logger) {
	if !v.IsFromMe || (v.Type != types.ReceiptTypeRead && v.Type != types.ReceiptTypeReadSelf) {
		return
	}
	ctx := context.Background()
	// Resolved the same way as handleMessage, to match how chats.jid is actually stored.
	chat := canonicalChatJID(ctx, client.Store, v.MessageSource)
	chatJID := chat.String()
	if err := messageStore.MarkChatRead(chatJID, v.Timestamp); err != nil {
		logger.Warnf("Failed to mark chat %s read from own-device receipt: %v", chatJID, err)
		return
	}
	logger.Debugf("Marked chat %s read up to %s (own-device receipt, type=%s)", chatJID, v.Timestamp, v.Type)
}

// Reacts to a whole chat being marked read or unread from another device (app-state sync).
func handleMarkChatAsRead(client *whatsmeow.Client, messageStore *MessageStore, v *events.MarkChatAsRead, logger waLog.Logger) {
	ctx := context.Background()
	// No per-stanza alt address here, so only the local LID<->PN cache can resolve it.
	chat := resolveAlt(ctx, client.Store, v.JID, types.JID{})
	chatJID := chat.String()

	if v.Action.GetRead() {
		upTo := v.Timestamp
		if ts := v.Action.GetMessageRange().GetLastMessageTimestamp(); ts > 0 {
			upTo = time.Unix(ts, 0)
		}
		if err := messageStore.MarkChatRead(chatJID, upTo); err != nil {
			logger.Warnf("Failed to mark chat %s read from app-state sync: %v", chatJID, err)
			return
		}
		logger.Infof("Chat %s marked read (up to %s) from another device", chatJID, upTo)
		return
	}

	// Mark-unread: resurface with exactly the newest incoming message unread (WhatsApp's own behavior).
	newest, err := messageStore.newestIncomingTimestamp(chatJID)
	if err != nil {
		logger.Warnf("Failed to look up newest message for %s: %v", chatJID, err)
		return
	}
	marker := time.Time{}
	if !newest.IsZero() {
		marker = newest.Add(-time.Second)
	}
	if err := messageStore.SetChatReadMarker(chatJID, marker); err != nil {
		logger.Warnf("Failed to set read marker for %s: %v", chatJID, err)
		return
	}
	logger.Infof("Chat %s marked unread from another device", chatJID)
}
