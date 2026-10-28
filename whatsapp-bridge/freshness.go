package main

import (
	"database/sql"
	"fmt"
)

// A send can name the newest message its author had read in the chat
// (SendMessageRequest.LatestMessageID). It is refused if anything has arrived
// since, both when submitted and again just before it goes out, so an agent
// never answers a conversation that has moved on without it.
//
// Messages this bridge sent itself through /api/send do not count as new:
// otherwise the second of two queued replies would be refused because of the
// first. They are listed in api_sent_messages rather than flagged on the
// messages row, which StoreMessage's INSERT OR REPLACE would reset.

func createFreshnessTables(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS api_sent_messages (
			id TEXT NOT NULL,
			chat_jid TEXT NOT NULL,
			PRIMARY KEY (id, chat_jid)
		);
		CREATE INDEX IF NOT EXISTS messages_chat_jid_idx ON messages(chat_jid);
	`)
	if err != nil {
		return fmt.Errorf("failed to create freshness tables: %v", err)
	}
	return nil
}

// MarkSentViaAPI records a message this bridge sent, so it is not mistaken
// for news by a later freshness check.
func (store *MessageStore) MarkSentViaAPI(id, chatJID string) error {
	_, err := store.db.Exec(`INSERT OR IGNORE INTO api_sent_messages (id, chat_jid) VALUES (?, ?)`, id, chatJID)
	return err
}

// latestMessageOrder is how "newest" is decided everywhere it matters for
// freshness: by timestamp, ties broken by arrival order.
const latestMessageOrder = `ORDER BY julianday(timestamp) DESC, rowid DESC`

// notSentViaAPI excludes this bridge's own sends from a query over messages m.
const notSentViaAPI = `NOT EXISTS (SELECT 1 FROM api_sent_messages a WHERE a.id = m.id AND a.chat_jid = m.chat_jid)`

// staleReplyMessage tells the agent what to do about a refused send.
func staleReplyMessage(chatJID string, n int, since string) string {
	what := fmt.Sprintf("%d new message(s) have arrived in %s since %s", n, chatJID, since)
	if since == "" {
		what = fmt.Sprintf("%s already has %d message(s), but no latest_message_id was given", chatJID, n)
	}
	return fmt.Sprintf("Not sent: %s. Your reply was written without them. "+
		"Call list_messages(chat_jid=%q) to read the new messages, rethink your reply in their light, "+
		"then send again with the latest_message_id that list_messages now reports.", what, chatJID)
}

// checkReplyFresh returns a non-empty refusal if req names a latest message
// and the chat has moved on since (or the id is not in that chat). A nil
// LatestMessageID means the sender asked for no check; an empty one means it
// believes the chat has no messages yet. err is a database failure, on which
// callers should refuse too rather than send blind.
func (store *MessageStore) checkReplyFresh(req SendMessageRequest) (refusal string, err error) {
	if req.LatestMessageID == nil {
		return "", nil
	}
	jid, err := parseRecipient(req.Recipient)
	if err != nil {
		return fmt.Sprintf("Error parsing recipient: %v", err), nil
	}
	chatJID := jid.String()
	latest := *req.LatestMessageID

	var n int
	if latest == "" {
		err = store.db.QueryRow(`SELECT COUNT(*) FROM messages m WHERE m.chat_jid = ? AND `+notSentViaAPI, chatJID).Scan(&n)
	} else {
		var found int
		if err = store.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE id = ? AND chat_jid = ?`, latest, chatJID).Scan(&found); err != nil {
			return "", err
		}
		if found == 0 {
			return fmt.Sprintf("Not sent: latest_message_id %s is not a message in %s. "+
				"Call list_messages(chat_jid=%q) and pass the latest_message_id it reports.", latest, chatJID, chatJID), nil
		}
		err = store.db.QueryRow(`
			SELECT COUNT(*) FROM messages m, messages ref
			WHERE ref.id = ? AND ref.chat_jid = ? AND m.chat_jid = ref.chat_jid
			  AND (julianday(m.timestamp) > julianday(ref.timestamp)
			       OR (julianday(m.timestamp) = julianday(ref.timestamp) AND m.rowid > ref.rowid))
			  AND `+notSentViaAPI, latest, chatJID).Scan(&n)
	}
	if err != nil {
		return "", err
	}
	if n > 0 {
		return staleReplyMessage(chatJID, n, latest), nil
	}
	return "", nil
}
