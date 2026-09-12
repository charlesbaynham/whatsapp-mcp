package main

// Read-side queries over messages.db, ported from the Python MCP server's
// whatsapp.py so that every client goes through the bridge's REST API and
// nothing else needs to open the database.

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const (
	readDefaultLimit = 20
	readMaxLimit     = 500
)

// MessageView is one message as the read API returns it.
type MessageView struct {
	ID         string    `json:"id"`
	ChatJID    string    `json:"chat_jid"`
	ChatName   string    `json:"chat_name"`
	Sender     string    `json:"sender"`
	SenderName string    `json:"sender_name"`
	Content    string    `json:"content"`
	Timestamp  time.Time `json:"timestamp"`
	IsFromMe   bool      `json:"is_from_me"`
	MediaType  string    `json:"media_type,omitempty"`
	Filename   string    `json:"filename,omitempty"`
	// Transcript is the spoken text of a voice note (content stays as
	// delivered); TranscriptionStatus is ok, pending, failed, timeout or disabled.
	Transcript          string `json:"transcript,omitempty"`
	TranscriptionStatus string `json:"transcription_status,omitempty"`
}

// ChatView is one chat as the read API returns it.
type ChatView struct {
	JID             string     `json:"jid"`
	Name            string     `json:"name"`
	LastMessageTime *time.Time `json:"last_message_time"`
	LastMessage     *string    `json:"last_message"`
	LastSender      *string    `json:"last_sender"`
	LastIsFromMe    *bool      `json:"last_is_from_me"`
	UnreadCount     int        `json:"unread_count"`
	LastReadAt      *time.Time `json:"last_read_at"`
	IsGroup         bool       `json:"is_group"`
}

// ContactView is one contact (a non-group chat row) as the read API returns it.
type ContactView struct {
	PhoneNumber string `json:"phone_number"`
	Name        string `json:"name"`
	JID         string `json:"jid"`
}

// MessageContextView is a message with its neighbours in the same chat.
type MessageContextView struct {
	Message MessageView   `json:"message"`
	Before  []MessageView `json:"before"`
	After   []MessageView `json:"after"`
}

// unreadCountExpr counts incoming messages newer than the chat's read marker
// (NULL marker = nothing read). julianday() because stored RFC3339 text
// doesn't compare safely across offsets.
func unreadCountExpr(chatsAlias string) string {
	return fmt.Sprintf(
		`(SELECT COUNT(*) FROM messages m WHERE m.chat_jid = %[1]s.jid AND m.is_from_me = 0 `+
			`AND (%[1]s.last_read_timestamp IS NULL OR julianday(m.timestamp) > julianday(%[1]s.last_read_timestamp)))`,
		chatsAlias)
}

// clampPage normalises limit/page into a LIMIT/OFFSET pair.
func clampPage(limit, page int) (int, int) {
	if limit <= 0 {
		limit = readDefaultLimit
	}
	if limit > readMaxLimit {
		limit = readMaxLimit
	}
	if page < 0 {
		page = 0
	}
	return limit, page * limit
}

const messageSelectColumns = `m.id, m.chat_jid, COALESCE(c.name, ''), m.sender, COALESCE(m.content, ''), m.timestamp, m.is_from_me,
	COALESCE(m.media_type, ''), COALESCE(m.filename, ''), COALESCE(m.transcript, ''), COALESCE(m.transcription_status, '')`

func scanMessageView(row interface{ Scan(dest ...any) error }) (MessageView, error) {
	var v MessageView
	var ts sql.NullTime
	if err := row.Scan(&v.ID, &v.ChatJID, &v.ChatName, &v.Sender, &v.Content, &ts, &v.IsFromMe, &v.MediaType, &v.Filename, &v.Transcript, &v.TranscriptionStatus); err != nil {
		return MessageView{}, err
	}
	if ts.Valid {
		v.Timestamp = ts.Time
	}
	return v, nil
}

// senderNamer resolves sender_name the way the Python server did: "Me" for
// our own messages, else the chat name for the sender's JID (exact, then a
// LIKE on the bare number), else the raw sender. Cached per request.
type senderNamer struct {
	store *MessageStore
	cache map[string]string
}

func (store *MessageStore) newSenderNamer() *senderNamer {
	return &senderNamer{store: store, cache: map[string]string{}}
}

func (n *senderNamer) name(sender string, isFromMe bool) string {
	if isFromMe {
		return "Me"
	}
	if v, ok := n.cache[sender]; ok {
		return v
	}
	name := sender
	var found sql.NullString
	err := n.store.db.QueryRow(`SELECT name FROM chats WHERE jid = ? LIMIT 1`, sender).Scan(&found)
	if err != nil || !found.Valid || found.String == "" {
		phone := sender
		if i := strings.Index(sender, "@"); i >= 0 {
			phone = sender[:i]
		}
		if phone != "" {
			err = n.store.db.QueryRow(`SELECT name FROM chats WHERE jid LIKE ? LIMIT 1`, "%"+phone+"%").Scan(&found)
		}
	}
	if err == nil && found.Valid && found.String != "" {
		name = found.String
	}
	n.cache[sender] = name
	return name
}

func (n *senderNamer) fill(msgs []MessageView) {
	for i := range msgs {
		msgs[i].SenderName = n.name(msgs[i].Sender, msgs[i].IsFromMe)
	}
}

func (store *MessageStore) queryMessages(where string, order string, args ...any) ([]MessageView, error) {
	q := `SELECT ` + messageSelectColumns + ` FROM messages m JOIN chats c ON m.chat_jid = c.jid`
	if where != "" {
		q += " WHERE " + where
	}
	q += " " + order
	rows, err := store.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MessageView{}
	for rows.Next() {
		v, err := scanMessageView(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ListMessagesParams mirrors the list_messages tool arguments.
type ListMessagesParams struct {
	After          *time.Time
	Before         *time.Time
	Sender         string
	ChatJID        string
	Query          string
	Limit          int
	Page           int
	IncludeContext bool
	ContextBefore  int
	ContextAfter   int
}

// ListMessages returns messages newest first. With IncludeContext, each hit is
// expanded to [before..., hit, after...] exactly as the Python server did.
func (store *MessageStore) ListMessages(p ListMessagesParams) ([]MessageView, error) {
	var where []string
	var args []any
	if p.After != nil {
		where = append(where, "julianday(m.timestamp) > julianday(?)")
		args = append(args, p.After.UTC().Format(time.RFC3339Nano))
	}
	if p.Before != nil {
		where = append(where, "julianday(m.timestamp) < julianday(?)")
		args = append(args, p.Before.UTC().Format(time.RFC3339Nano))
	}
	if p.Sender != "" {
		where = append(where, "m.sender = ?")
		args = append(args, p.Sender)
	}
	if p.ChatJID != "" {
		where = append(where, "m.chat_jid = ?")
		args = append(args, p.ChatJID)
	}
	if p.Query != "" {
		where = append(where, "LOWER(m.content) LIKE LOWER(?)")
		args = append(args, "%"+p.Query+"%")
	}
	limit, offset := clampPage(p.Limit, p.Page)
	args = append(args, limit, offset)

	hits, err := store.queryMessages(strings.Join(where, " AND "), "ORDER BY julianday(m.timestamp) DESC LIMIT ? OFFSET ?", args...)
	if err != nil {
		return nil, err
	}

	namer := store.newSenderNamer()
	if !p.IncludeContext || len(hits) == 0 {
		namer.fill(hits)
		return hits, nil
	}

	out := make([]MessageView, 0, len(hits)*(1+p.ContextBefore+p.ContextAfter))
	for _, hit := range hits {
		ctx, err := store.messageContextFor(hit, p.ContextBefore, p.ContextAfter)
		if err != nil {
			return nil, err
		}
		out = append(out, ctx.Before...)
		out = append(out, ctx.Message)
		out = append(out, ctx.After...)
	}
	namer.fill(out)
	return out, nil
}

// GetMessageContext finds a message by id (optionally disambiguated by chat)
// and returns it with `before` older and `after` newer messages from its chat.
func (store *MessageStore) GetMessageContext(messageID, chatJID string, before, after int) (MessageContextView, bool, error) {
	where := "m.id = ?"
	args := []any{messageID}
	if chatJID != "" {
		where += " AND m.chat_jid = ?"
		args = append(args, chatJID)
	}
	found, err := store.queryMessages(where, "LIMIT 1", args...)
	if err != nil {
		return MessageContextView{}, false, err
	}
	if len(found) == 0 {
		return MessageContextView{}, false, nil
	}
	ctx, err := store.messageContextFor(found[0], before, after)
	if err != nil {
		return MessageContextView{}, false, err
	}
	namer := store.newSenderNamer()
	ctx.Message.SenderName = namer.name(ctx.Message.Sender, ctx.Message.IsFromMe)
	namer.fill(ctx.Before)
	namer.fill(ctx.After)
	return ctx, true, nil
}

func (store *MessageStore) messageContextFor(target MessageView, before, after int) (MessageContextView, error) {
	if before < 0 {
		before = 0
	}
	if after < 0 {
		after = 0
	}
	ts := target.Timestamp.UTC().Format(time.RFC3339Nano)
	older, err := store.queryMessages(
		"m.chat_jid = ? AND julianday(m.timestamp) < julianday(?)",
		"ORDER BY julianday(m.timestamp) DESC LIMIT ?", target.ChatJID, ts, before)
	if err != nil {
		return MessageContextView{}, err
	}
	newer, err := store.queryMessages(
		"m.chat_jid = ? AND julianday(m.timestamp) > julianday(?)",
		"ORDER BY julianday(m.timestamp) ASC LIMIT ?", target.ChatJID, ts, after)
	if err != nil {
		return MessageContextView{}, err
	}
	// Python returned `before` newest-first straight from the DESC query;
	// chronological order is more useful to every consumer, so reverse it.
	for i, j := 0, len(older)-1; i < j; i, j = i+1, j-1 {
		older[i], older[j] = older[j], older[i]
	}
	return MessageContextView{Message: target, Before: older, After: newer}, nil
}

// ListChatsParams mirrors the list_chats tool arguments.
type ListChatsParams struct {
	Query              string
	Limit              int
	Page               int
	IncludeLastMessage bool
	SortBy             string // "last_active" (default) or "name"
	UnreadOnly         bool
}

const chatSelectColumns = `c.jid, COALESCE(c.name, ''), c.last_message_time, m.content, m.sender, m.is_from_me, %s, c.last_read_timestamp`

func chatSelect(includeLastMessage bool) string {
	q := fmt.Sprintf(`SELECT `+chatSelectColumns+` FROM chats c`, unreadCountExpr("c"))
	if includeLastMessage {
		q += ` LEFT JOIN messages m ON c.jid = m.chat_jid AND c.last_message_time = m.timestamp`
	} else {
		// Keep the column list stable: join nothing, select NULLs.
		q = fmt.Sprintf(`SELECT c.jid, COALESCE(c.name, ''), c.last_message_time, NULL, NULL, NULL, %s, c.last_read_timestamp FROM chats c`, unreadCountExpr("c"))
	}
	return q
}

func scanChatView(row interface{ Scan(dest ...any) error }) (ChatView, error) {
	var v ChatView
	var lastTime, lastRead sql.NullTime
	var lastMsg, lastSender sql.NullString
	var lastFromMe sql.NullBool
	if err := row.Scan(&v.JID, &v.Name, &lastTime, &lastMsg, &lastSender, &lastFromMe, &v.UnreadCount, &lastRead); err != nil {
		return ChatView{}, err
	}
	if lastTime.Valid {
		t := lastTime.Time
		v.LastMessageTime = &t
	}
	if lastRead.Valid {
		t := lastRead.Time
		v.LastReadAt = &t
	}
	if lastMsg.Valid {
		s := lastMsg.String
		v.LastMessage = &s
	}
	if lastSender.Valid {
		s := lastSender.String
		v.LastSender = &s
	}
	if lastFromMe.Valid {
		b := lastFromMe.Bool
		v.LastIsFromMe = &b
	}
	v.IsGroup = strings.HasSuffix(v.JID, "@g.us")
	return v, nil
}

func (store *MessageStore) queryChats(q string, args ...any) ([]ChatView, error) {
	rows, err := store.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ChatView{}
	for rows.Next() {
		v, err := scanChatView(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ListChats returns chats, most recently active first by default.
func (store *MessageStore) ListChats(p ListChatsParams) ([]ChatView, error) {
	q := chatSelect(p.IncludeLastMessage)
	var where []string
	var args []any
	if p.Query != "" {
		where = append(where, "(LOWER(c.name) LIKE LOWER(?) OR c.jid LIKE ?)")
		args = append(args, "%"+p.Query+"%", "%"+p.Query+"%")
	}
	if p.UnreadOnly {
		where = append(where, unreadCountExpr("c")+" > 0")
	}
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	if p.SortBy == "name" {
		q += " ORDER BY c.name"
	} else {
		q += " ORDER BY julianday(c.last_message_time) DESC"
	}
	limit, offset := clampPage(p.Limit, p.Page)
	q += " LIMIT ? OFFSET ?"
	args = append(args, limit, offset)
	return store.queryChats(q, args...)
}

// GetChat returns one chat by JID.
func (store *MessageStore) GetChat(chatJID string, includeLastMessage bool) (ChatView, bool, error) {
	chats, err := store.queryChats(chatSelect(includeLastMessage)+" WHERE c.jid = ? LIMIT 1", chatJID)
	if err != nil || len(chats) == 0 {
		return ChatView{}, false, err
	}
	return chats[0], true, nil
}

// GetDirectChatByPhone finds the 1:1 chat whose JID contains the number.
func (store *MessageStore) GetDirectChatByPhone(phone string) (ChatView, bool, error) {
	chats, err := store.queryChats(chatSelect(true)+" WHERE c.jid LIKE ? AND c.jid NOT LIKE '%@g.us' LIMIT 1", "%"+phone+"%")
	if err != nil || len(chats) == 0 {
		return ChatView{}, false, err
	}
	return chats[0], true, nil
}

// SearchContacts matches non-group chats by name or JID.
func (store *MessageStore) SearchContacts(query string) ([]ContactView, error) {
	pattern := "%" + query + "%"
	rows, err := store.db.Query(
		`SELECT DISTINCT jid, COALESCE(name, '') FROM chats
		 WHERE (LOWER(name) LIKE LOWER(?) OR LOWER(jid) LIKE LOWER(?)) AND jid NOT LIKE '%@g.us'
		 ORDER BY name, jid LIMIT 50`, pattern, pattern)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ContactView{}
	for rows.Next() {
		var c ContactView
		if err := rows.Scan(&c.JID, &c.Name); err != nil {
			return nil, err
		}
		c.PhoneNumber = c.JID
		if i := strings.Index(c.JID, "@"); i >= 0 {
			c.PhoneNumber = c.JID[:i]
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GetContactChats lists every chat the contact has sent to, plus their 1:1 chat.
func (store *MessageStore) GetContactChats(jid string, limit, page int) ([]ChatView, error) {
	limit, offset := clampPage(limit, page)
	q := chatSelect(true) + `
		WHERE c.jid = ? OR c.jid IN (SELECT DISTINCT chat_jid FROM messages WHERE sender = ?)
		ORDER BY julianday(c.last_message_time) DESC LIMIT ? OFFSET ?`
	return store.queryChats(q, jid, jid, limit, offset)
}

// GetLastInteraction returns the newest message sent by or to the contact.
func (store *MessageStore) GetLastInteraction(jid string) (MessageView, bool, error) {
	msgs, err := store.queryMessages("m.sender = ? OR c.jid = ?", "ORDER BY julianday(m.timestamp) DESC LIMIT 1", jid, jid)
	if err != nil || len(msgs) == 0 {
		return MessageView{}, false, err
	}
	store.newSenderNamer().fill(msgs)
	return msgs[0], true, nil
}
