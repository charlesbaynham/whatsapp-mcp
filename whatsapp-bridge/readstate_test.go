package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow/types"
)

// oldChatsAndMessagesDDL is the pre-read-state-feature schema, copied
// verbatim (minus last_read_timestamp) from NewMessageStore, so tests can
// exercise the upgrade path against a DB that predates this feature.
const oldChatsAndMessagesDDL = `
	CREATE TABLE IF NOT EXISTS chats (
		jid TEXT PRIMARY KEY,
		name TEXT,
		last_message_time TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS messages (
		id TEXT,
		chat_jid TEXT,
		sender TEXT,
		content TEXT,
		timestamp TIMESTAMP,
		is_from_me BOOLEAN,
		media_type TEXT,
		filename TEXT,
		url TEXT,
		media_key BLOB,
		file_sha256 BLOB,
		file_enc_sha256 BLOB,
		file_length INTEGER,
		PRIMARY KEY (id, chat_jid),
		FOREIGN KEY (chat_jid) REFERENCES chats(jid)
	);
`

// readMarker reads a chat's last_read_timestamp directly, bypassing the
// MessageStore API, for assertions about the migration/backfill.
func readMarker(t *testing.T, store *MessageStore, chatJID string) sql.NullTime {
	t.Helper()
	var marker sql.NullTime
	if err := store.db.QueryRow("SELECT last_read_timestamp FROM chats WHERE jid = ?", chatJID).Scan(&marker); err != nil {
		t.Fatalf("failed to read last_read_timestamp: %v", err)
	}
	return marker
}

func TestNewMessageStoreMigratesOldSchemaAndBackfills(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "messages.db")

	t1 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// Hand-build a pre-migration DB: a chat with a last_message_time and no
	// last_read_timestamp column at all.
	raw, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on", dbPath))
	if err != nil {
		t.Fatalf("failed to open raw db: %v", err)
	}
	if _, err := raw.Exec(oldChatsAndMessagesDDL); err != nil {
		t.Fatalf("failed to create old-schema tables: %v", err)
	}
	if _, err := raw.Exec("INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)", "111@s.whatsapp.net", "Alice", t1); err != nil {
		t.Fatalf("failed to insert chat: %v", err)
	}
	if _, err := raw.Exec(
		`INSERT INTO messages (id, chat_jid, sender, content, timestamp, is_from_me) VALUES (?, ?, ?, ?, ?, ?)`,
		"m1", "111@s.whatsapp.net", "111", "hello", t1, false,
	); err != nil {
		t.Fatalf("failed to insert message: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("failed to close raw db: %v", err)
	}

	// Opening via NewMessageStore must add the column and backfill it to
	// last_message_time - "existing messages count as read".
	store, err := NewMessageStore(dir)
	if err != nil {
		t.Fatalf("NewMessageStore failed to migrate old schema: %v", err)
	}
	marker := readMarker(t, store, "111@s.whatsapp.net")
	if !marker.Valid {
		t.Fatalf("last_read_timestamp is NULL after migration, want backfilled to %v", t1)
	}
	if !marker.Time.Equal(t1) {
		t.Fatalf("last_read_timestamp = %v, want %v", marker.Time, t1)
	}
	unread, err := store.GetUnreadMessages("111@s.whatsapp.net")
	if err != nil {
		t.Fatalf("GetUnreadMessages failed: %v", err)
	}
	if len(unread) != 0 {
		t.Fatalf("expected the pre-existing message to count as read after migration, got %d unread", len(unread))
	}
	if err := store.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	// Simulate a new incoming message arriving after the migration, then
	// reopening the store (the backfill must NEVER run again, or it would
	// reset last_read_timestamp to last_message_time and silently mark the
	// new message read too).
	store2, err := NewMessageStore(dir)
	if err != nil {
		t.Fatalf("second NewMessageStore open failed: %v", err)
	}
	t2 := t1.Add(1 * time.Hour)
	if err := store2.StoreMessage("m2", "111@s.whatsapp.net", "111", "new message", t2, false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("failed to store new message: %v", err)
	}
	// A realistic flow also advances last_message_time via StoreChat; the
	// upsert must not touch last_read_timestamp (covered again, more
	// directly, in TestStoreChatUpsertPreservesReadMarker).
	if err := store2.StoreChat("111@s.whatsapp.net", "Alice", t2); err != nil {
		t.Fatalf("StoreChat failed: %v", err)
	}
	if err := store2.Close(); err != nil {
		t.Fatalf("failed to close store: %v", err)
	}

	store3, err := NewMessageStore(dir)
	if err != nil {
		t.Fatalf("third NewMessageStore open failed: %v", err)
	}
	defer store3.Close()

	marker3 := readMarker(t, store3, "111@s.whatsapp.net")
	if !marker3.Valid || !marker3.Time.Equal(t1) {
		t.Fatalf("last_read_timestamp after reopening = %v, want unchanged at %v (backfill must not rerun)", marker3, t1)
	}
	unread3, err := store3.GetUnreadMessages("111@s.whatsapp.net")
	if err != nil {
		t.Fatalf("GetUnreadMessages failed: %v", err)
	}
	if len(unread3) != 1 || unread3[0].ID != "m2" {
		t.Fatalf("GetUnreadMessages = %+v, want exactly [m2] unread", unread3)
	}
}

func TestStoreChatUpsertPreservesReadMarker(t *testing.T) {
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMessageStore failed: %v", err)
	}
	defer store.Close()

	const chatJID = "222@s.whatsapp.net"
	t1 := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	if err := store.StoreChat(chatJID, "Bob", t1); err != nil {
		t.Fatalf("StoreChat failed: %v", err)
	}
	if err := store.MarkChatRead(chatJID, t1); err != nil {
		t.Fatalf("MarkChatRead failed: %v", err)
	}

	t2 := t1.Add(1 * time.Hour)
	if err := store.StoreChat(chatJID, "Bob Renamed", t2); err != nil {
		t.Fatalf("second StoreChat failed: %v", err)
	}

	marker := readMarker(t, store, chatJID)
	if !marker.Valid || !marker.Time.Equal(t1) {
		t.Fatalf("last_read_timestamp = %v after StoreChat upsert, want unchanged at %v", marker, t1)
	}

	var name string
	var lastMessageTime time.Time
	if err := store.db.QueryRow("SELECT name, last_message_time FROM chats WHERE jid = ?", chatJID).Scan(&name, &lastMessageTime); err != nil {
		t.Fatalf("failed to read chat row: %v", err)
	}
	if name != "Bob Renamed" {
		t.Fatalf("name = %q, want %q (StoreChat upsert should still update name)", name, "Bob Renamed")
	}
	if !lastMessageTime.Equal(t2) {
		t.Fatalf("last_message_time = %v, want %v", lastMessageTime, t2)
	}
}

func TestGetUnreadMessages(t *testing.T) {
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMessageStore failed: %v", err)
	}
	defer store.Close()

	const chatJID = "333@s.whatsapp.net"
	t0 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	if err := store.StoreChat(chatJID, "Carol", t0); err != nil {
		t.Fatalf("StoreChat failed: %v", err)
	}
	if err := store.MarkChatRead(chatJID, t0); err != nil {
		t.Fatalf("MarkChatRead failed: %v", err)
	}

	// Excluded: outgoing message after the marker.
	if err := store.StoreMessage("out1", chatJID, "us", "sent by us", t0.Add(10*time.Second), true, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("StoreMessage out1 failed: %v", err)
	}
	// Excluded: incoming message before the marker.
	if err := store.StoreMessage("before1", chatJID, "333", "old", t0.Add(-10*time.Second), false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("StoreMessage before1 failed: %v", err)
	}
	// Included: incoming message after the marker, timestamp written in a
	// non-UTC offset to prove julianday() (not raw string comparison)
	// governs the comparison.
	offsetZone := time.FixedZone("TEST+0300", 3*60*60)
	included := t0.Add(20 * time.Second).In(offsetZone)
	if err := store.StoreMessage("after1", chatJID, "333", "new", included, false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("StoreMessage after1 failed: %v", err)
	}

	unread, err := store.GetUnreadMessages(chatJID)
	if err != nil {
		t.Fatalf("GetUnreadMessages failed: %v", err)
	}
	if len(unread) != 1 {
		t.Fatalf("GetUnreadMessages returned %d messages, want 1: %+v", len(unread), unread)
	}
	if unread[0].ID != "after1" {
		t.Fatalf("unread message ID = %q, want %q", unread[0].ID, "after1")
	}
	if unread[0].Sender != "333" {
		t.Fatalf("unread message Sender = %q, want %q", unread[0].Sender, "333")
	}
	if !unread[0].Timestamp.Equal(included) {
		t.Fatalf("unread message Timestamp = %v, want %v", unread[0].Timestamp, included)
	}
}

func TestGetUnreadMessagesNullMarkerMeansAllUnread(t *testing.T) {
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMessageStore failed: %v", err)
	}
	defer store.Close()

	const chatJID = "444@s.whatsapp.net"
	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	if err := store.StoreChat(chatJID, "Dave", t0); err != nil {
		t.Fatalf("StoreChat failed: %v", err)
	}
	if err := store.StoreMessage("m1", chatJID, "444", "hi", t0, false, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatalf("StoreMessage failed: %v", err)
	}

	unread, err := store.GetUnreadMessages(chatJID)
	if err != nil {
		t.Fatalf("GetUnreadMessages failed: %v", err)
	}
	if len(unread) != 1 || unread[0].ID != "m1" {
		t.Fatalf("GetUnreadMessages = %+v, want exactly [m1] with a NULL marker", unread)
	}
}

func TestMarkChatReadIsMonotonic(t *testing.T) {
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMessageStore failed: %v", err)
	}
	defer store.Close()

	const chatJID = "555@s.whatsapp.net"
	t0 := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if err := store.StoreChat(chatJID, "Erin", t0); err != nil {
		t.Fatalf("StoreChat failed: %v", err)
	}

	// Marker starts NULL; a first MarkChatRead call must set it.
	t1 := t0.Add(1 * time.Hour)
	if err := store.MarkChatRead(chatJID, t1); err != nil {
		t.Fatalf("MarkChatRead(t1) failed: %v", err)
	}
	marker := readMarker(t, store, chatJID)
	if !marker.Valid || !marker.Time.Equal(t1) {
		t.Fatalf("marker after first MarkChatRead = %v, want %v", marker, t1)
	}

	// An earlier timestamp must not move the marker backward.
	earlier := t0.Add(30 * time.Minute)
	if err := store.MarkChatRead(chatJID, earlier); err != nil {
		t.Fatalf("MarkChatRead(earlier) failed: %v", err)
	}
	marker = readMarker(t, store, chatJID)
	if !marker.Valid || !marker.Time.Equal(t1) {
		t.Fatalf("marker after earlier MarkChatRead = %v, want unchanged at %v", marker, t1)
	}

	// A later timestamp must advance it.
	t2 := t0.Add(2 * time.Hour)
	if err := store.MarkChatRead(chatJID, t2); err != nil {
		t.Fatalf("MarkChatRead(t2) failed: %v", err)
	}
	marker = readMarker(t, store, chatJID)
	if !marker.Valid || !marker.Time.Equal(t2) {
		t.Fatalf("marker after later MarkChatRead = %v, want %v", marker, t2)
	}
}

func TestSetChatReadMarkerCanClearToNull(t *testing.T) {
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMessageStore failed: %v", err)
	}
	defer store.Close()

	const chatJID = "666@s.whatsapp.net"
	t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	if err := store.StoreChat(chatJID, "Frank", t0); err != nil {
		t.Fatalf("StoreChat failed: %v", err)
	}
	if err := store.MarkChatRead(chatJID, t0); err != nil {
		t.Fatalf("MarkChatRead failed: %v", err)
	}
	if err := store.SetChatReadMarker(chatJID, time.Time{}); err != nil {
		t.Fatalf("SetChatReadMarker(zero) failed: %v", err)
	}
	marker := readMarker(t, store, chatJID)
	if marker.Valid {
		t.Fatalf("marker = %v after SetChatReadMarker(zero), want NULL", marker)
	}

	// And it can move backward, unlike MarkChatRead.
	if err := store.MarkChatRead(chatJID, t0.Add(2*time.Hour)); err != nil {
		t.Fatalf("MarkChatRead failed: %v", err)
	}
	earlier := t0.Add(1 * time.Hour)
	if err := store.SetChatReadMarker(chatJID, earlier); err != nil {
		t.Fatalf("SetChatReadMarker(earlier) failed: %v", err)
	}
	marker = readMarker(t, store, chatJID)
	if !marker.Valid || !marker.Time.Equal(earlier) {
		t.Fatalf("marker = %v after SetChatReadMarker(earlier), want %v", marker, earlier)
	}
}

func TestGroupUnreadBySender(t *testing.T) {
	cases := []struct {
		name        string
		unread      []UnreadMessage
		wantOrder   []string
		wantBySizes map[string]int
	}{
		{
			name:        "empty",
			unread:      nil,
			wantOrder:   nil,
			wantBySizes: map[string]int{},
		},
		{
			name: "all one sender",
			unread: []UnreadMessage{
				{ID: "a", Sender: "111"},
				{ID: "b", Sender: "111"},
			},
			wantOrder:   []string{"111"},
			wantBySizes: map[string]int{"111": 2},
		},
		{
			name: "interleaved senders keep first-seen order",
			unread: []UnreadMessage{
				{ID: "a", Sender: "111"},
				{ID: "b", Sender: "222"},
				{ID: "c", Sender: "111"},
				{ID: "d", Sender: "333"},
				{ID: "e", Sender: "222"},
			},
			wantOrder:   []string{"111", "222", "333"},
			wantBySizes: map[string]int{"111": 2, "222": 2, "333": 1},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			order, bySender := groupUnreadBySender(c.unread)
			if len(order) != len(c.wantOrder) {
				t.Fatalf("order = %v, want %v", order, c.wantOrder)
			}
			for i, s := range c.wantOrder {
				if order[i] != s {
					t.Fatalf("order = %v, want %v", order, c.wantOrder)
				}
			}
			if len(bySender) != len(c.wantBySizes) {
				t.Fatalf("bySender has %d senders, want %d: %v", len(bySender), len(c.wantBySizes), bySender)
			}
			for sender, wantLen := range c.wantBySizes {
				if len(bySender[sender]) != wantLen {
					t.Fatalf("bySender[%q] has %d ids, want %d", sender, len(bySender[sender]), wantLen)
				}
			}
		})
	}

	// IDs within a sender preserve GetUnreadMessages' (oldest-first) order.
	unread := []UnreadMessage{
		{ID: "old", Sender: "111"},
		{ID: "new", Sender: "111"},
	}
	_, bySender := groupUnreadBySender(unread)
	ids := bySender["111"]
	if len(ids) != 2 || ids[0] != types.MessageID("old") || ids[1] != types.MessageID("new") {
		t.Fatalf("bySender[111] = %v, want [old new] in order", ids)
	}
}

func TestSenderJIDForMarkRead(t *testing.T) {
	oneOnOne := jid("111", types.DefaultUserServer)
	group := jid("999", types.GroupServer)

	cases := []struct {
		name       string
		chat       types.JID
		senderUser string
		want       types.JID
	}{
		{"1:1 chat: sender is the chat JID", oneOnOne, "111", oneOnOne},
		{"group chat: sender rebuilt with default user server", group, "222", jid("222", types.DefaultUserServer)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := senderJIDForMarkRead(c.chat, c.senderUser)
			if got != c.want {
				t.Fatalf("senderJIDForMarkRead(%v, %q) = %v, want %v", c.chat, c.senderUser, got, c.want)
			}
		})
	}
}
