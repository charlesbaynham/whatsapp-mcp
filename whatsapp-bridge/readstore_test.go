package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Fixture ported from the Python server's test_unread.py:
//
//	chat A: marker NULL, 2 incoming + 1 outgoing        -> unread 2
//	chat B: marker after all messages                   -> unread 0
//	chat C: marker between two incoming messages, one of them stored with a
//	        non-UTC offset a raw-string compare gets wrong -> unread 1
func newReadFixture(t *testing.T) *MessageStore {
	t.Helper()
	store := newTestStore(t)
	mustTime := func(s string) time.Time {
		tm, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return tm
	}
	sender := "447700900000"
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	msg := func(id, chat, from, content string, ts string, fromMe bool) {
		must(store.StoreMessage(id, chat, from, content, mustTime(ts), fromMe, "", "", "", nil, nil, nil, 0))
	}

	must(store.StoreChat("a@s.whatsapp.net", "Alice", mustTime("2026-09-09T09:00:00Z")))
	msg("a1", "a@s.whatsapp.net", sender, "hi", "2026-09-09T07:00:00Z", false)
	msg("a2", "a@s.whatsapp.net", sender, "there", "2026-09-09T08:00:00Z", false)
	msg("a3", "a@s.whatsapp.net", "me", "reply", "2026-09-09T09:00:00Z", true)

	must(store.StoreChat("b@s.whatsapp.net", "Bob", mustTime("2026-09-08T10:00:00Z")))
	must(store.SetChatReadMarker("b@s.whatsapp.net", mustTime("2026-09-08T12:00:00Z")))
	msg("b1", "b@s.whatsapp.net", sender, "yo", "2026-09-08T09:00:00Z", false)
	msg("b2", "b@s.whatsapp.net", sender, "sup", "2026-09-08T10:00:00Z", false)

	must(store.StoreChat("c@s.whatsapp.net", "Carol", mustTime("2026-09-09T18:00:00-05:00")))
	must(store.SetChatReadMarker("c@s.whatsapp.net", mustTime("2026-09-09T12:00:00Z")))
	msg("c1", "c@s.whatsapp.net", sender, "before", "2026-09-09T10:00:00Z", false)
	// 08:00-05:00 is 13:00Z: after the 12:00Z marker although lexically earlier.
	msg("c2", "c@s.whatsapp.net", sender, "after", "2026-09-09T08:00:00-05:00", false)

	must(store.StoreChat("g@g.us", "Group", mustTime("2026-09-10T10:00:00Z")))
	msg("g1", "g@g.us", sender, "group hello", "2026-09-10T10:00:00Z", false)
	return store
}

func TestGetChatUnreadCounts(t *testing.T) {
	store := newReadFixture(t)
	for jid, want := range map[string]int{"a@s.whatsapp.net": 2, "b@s.whatsapp.net": 0, "c@s.whatsapp.net": 1} {
		chat, found, err := store.GetChat(jid, true)
		if err != nil || !found {
			t.Fatalf("GetChat(%s): found=%v err=%v", jid, found, err)
		}
		if chat.UnreadCount != want {
			t.Errorf("%s unread = %d, want %d", jid, chat.UnreadCount, want)
		}
	}
	a, _, _ := store.GetChat("a@s.whatsapp.net", true)
	if a.LastReadAt != nil {
		t.Errorf("chat A last_read_at = %v, want nil", a.LastReadAt)
	}
	if a.LastMessage == nil || *a.LastMessage != "reply" || a.LastIsFromMe == nil || !*a.LastIsFromMe {
		t.Errorf("chat A last message = %+v", a)
	}
	b, _, _ := store.GetChat("b@s.whatsapp.net", false)
	if b.LastReadAt == nil || b.LastMessage != nil {
		t.Errorf("chat B without last message: %+v", b)
	}
}

func TestListChatsUnreadOnly(t *testing.T) {
	store := newReadFixture(t)
	jids := func(chats []ChatView) map[string]int {
		m := map[string]int{}
		for _, c := range chats {
			m[c.JID] = c.UnreadCount
		}
		return m
	}
	all, err := store.ListChats(ListChatsParams{Limit: 50, IncludeLastMessage: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Fatalf("ListChats returned %d chats, want 4", len(all))
	}
	if all[0].JID != "g@g.us" || !all[0].IsGroup {
		t.Errorf("most recent chat = %+v, want the group", all[0])
	}
	unread, err := store.ListChats(ListChatsParams{Limit: 50, IncludeLastMessage: true, UnreadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	got := jids(unread)
	if len(got) != 3 || got["a@s.whatsapp.net"] != 2 || got["c@s.whatsapp.net"] != 1 || got["g@g.us"] != 1 {
		t.Errorf("unread chats = %v", got)
	}
	named, _ := store.ListChats(ListChatsParams{Query: "car", Limit: 50})
	if len(named) != 1 || named[0].JID != "c@s.whatsapp.net" {
		t.Errorf("query=car -> %+v", named)
	}
	byName, _ := store.ListChats(ListChatsParams{SortBy: "name", Limit: 2})
	if len(byName) != 2 || byName[0].Name != "Alice" || byName[1].Name != "Bob" {
		t.Errorf("sort_by=name limit=2 -> %+v", byName)
	}
}

func TestListMessagesAndContext(t *testing.T) {
	store := newReadFixture(t)
	msgs, err := store.ListMessages(ListMessagesParams{ChatJID: "a@s.whatsapp.net", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 || msgs[0].ID != "a3" || msgs[2].ID != "a1" {
		t.Fatalf("messages newest first: %+v", msgs)
	}
	if msgs[0].SenderName != "Me" || msgs[1].SenderName != "447700900000" || msgs[1].ChatName != "Alice" {
		t.Errorf("sender/chat names: %+v", msgs)
	}

	after := time.Date(2026, 9, 9, 7, 30, 0, 0, time.UTC)
	msgs, _ = store.ListMessages(ListMessagesParams{ChatJID: "a@s.whatsapp.net", After: &after, Limit: 10})
	if len(msgs) != 2 {
		t.Errorf("after filter: got %d messages, want 2", len(msgs))
	}
	msgs, _ = store.ListMessages(ListMessagesParams{Query: "HELLO", Limit: 10})
	if len(msgs) != 1 || msgs[0].ID != "g1" {
		t.Errorf("query filter: %+v", msgs)
	}

	// Offset-aware: c2 (13:00Z) must sort after c1 (10:00Z).
	msgs, _ = store.ListMessages(ListMessagesParams{ChatJID: "c@s.whatsapp.net", Limit: 10})
	if len(msgs) != 2 || msgs[0].ID != "c2" {
		t.Errorf("offset ordering: %+v", msgs)
	}

	ctx, found, err := store.GetMessageContext("a2", "", 5, 5)
	if err != nil || !found {
		t.Fatalf("GetMessageContext: found=%v err=%v", found, err)
	}
	if ctx.Message.ID != "a2" || len(ctx.Before) != 1 || ctx.Before[0].ID != "a1" || len(ctx.After) != 1 || ctx.After[0].ID != "a3" {
		t.Errorf("context: %+v", ctx)
	}
	if _, found, _ := store.GetMessageContext("a2", "b@s.whatsapp.net", 1, 1); found {
		t.Error("context with wrong chat_jid should not be found")
	}

	expanded, _ := store.ListMessages(ListMessagesParams{ChatJID: "a@s.whatsapp.net", Limit: 1, IncludeContext: true, ContextBefore: 1, ContextAfter: 1})
	if len(expanded) != 2 || expanded[0].ID != "a2" || expanded[1].ID != "a3" {
		t.Errorf("include_context expansion: %+v", expanded)
	}
}

func TestContactsAndInteractions(t *testing.T) {
	store := newReadFixture(t)
	contacts, err := store.SearchContacts("o")
	if err != nil {
		t.Fatal(err)
	}
	// Bob and Carol match on name; the group is excluded.
	if len(contacts) != 2 || contacts[0].Name != "Bob" || contacts[0].PhoneNumber != "b" || contacts[1].Name != "Carol" {
		t.Errorf("contacts: %+v", contacts)
	}

	chats, err := store.GetContactChats("447700900000", 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chats) != 4 || chats[0].JID != "g@g.us" {
		t.Errorf("contact chats: %+v", chats)
	}

	last, found, _ := store.GetLastInteraction("447700900000")
	if !found || last.ID != "g1" {
		t.Errorf("last interaction: found=%v %+v", found, last)
	}
	direct, found, _ := store.GetDirectChatByPhone("a")
	if !found || direct.JID != "a@s.whatsapp.net" {
		t.Errorf("direct chat: found=%v %+v", found, direct)
	}
}

func TestReadRoutes(t *testing.T) {
	store := newReadFixture(t)
	mux := http.NewServeMux()
	registerReadRoutes(mux, store)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	get := func(path string, want int, into any) {
		t.Helper()
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
		if into != nil {
			if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
				t.Fatalf("GET %s: decode: %v", path, err)
			}
		}
	}

	var chats []ChatView
	get("/api/chats?limit=50", 200, &chats)
	if len(chats) != 4 {
		t.Errorf("/api/chats: %d chats", len(chats))
	}
	get("/api/chats/unread", 200, &chats)
	if len(chats) != 3 {
		t.Errorf("/api/chats/unread: %d chats", len(chats))
	}
	var chat ChatView
	get("/api/chats/a@s.whatsapp.net", 200, &chat)
	if chat.UnreadCount != 2 {
		t.Errorf("/api/chats/{jid}: %+v", chat)
	}
	get("/api/chats/nope@s.whatsapp.net", 404, nil)
	get("/api/chats/by-phone/b", 200, &chat)
	if chat.Name != "Bob" {
		t.Errorf("/api/chats/by-phone: %+v", chat)
	}
	get("/api/chats?limit=x", 400, nil)

	var contacts []ContactView
	get("/api/contacts?query=ali", 200, &contacts)
	if len(contacts) != 1 || contacts[0].Name != "Alice" {
		t.Errorf("/api/contacts: %+v", contacts)
	}
	get("/api/contacts/447700900000/chats", 200, &chats)
	if len(chats) != 4 {
		t.Errorf("/api/contacts/{jid}/chats: %d", len(chats))
	}
	var msg MessageView
	get("/api/contacts/447700900000/last-interaction", 200, &msg)
	if msg.ID != "g1" {
		t.Errorf("last-interaction: %+v", msg)
	}
	get("/api/contacts/nobody/last-interaction", 404, nil)

	var msgs []MessageView
	get("/api/messages?chat_jid=a@s.whatsapp.net&after=2026-09-09T07:30:00Z", 200, &msgs)
	if len(msgs) != 2 {
		t.Errorf("/api/messages after: %+v", msgs)
	}
	get("/api/messages?after=notadate", 400, nil)

	var ctx MessageContextView
	get("/api/messages/a2/context?before=1&after=1", 200, &ctx)
	if ctx.Message.ID != "a2" || len(ctx.Before) != 1 || len(ctx.After) != 1 {
		t.Errorf("context: %+v", ctx)
	}
	get("/api/messages/zzz/context", 404, nil)

	resp, _ := http.Post(srv.URL+"/api/chats", "application/json", nil)
	if resp.StatusCode != 405 {
		t.Errorf("POST /api/chats = %d, want 405", resp.StatusCode)
	}
}
