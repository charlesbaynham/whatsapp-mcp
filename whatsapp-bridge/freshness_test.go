package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const freshChat = "447700900000@s.whatsapp.net"

func freshStore(t *testing.T) *MessageStore {
	t.Helper()
	store, err := NewMessageStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.StoreChat(freshChat, "Alice", time.Now()); err != nil {
		t.Fatal(err)
	}
	return store
}

func addMsg(t *testing.T, store *MessageStore, id string, at time.Time, fromMe bool) {
	t.Helper()
	if err := store.StoreMessage(id, freshChat, "447700900000", "text "+id, at, fromMe, "", "", "", nil, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
}

func replyTo(latest string) SendMessageRequest {
	return SendMessageRequest{Recipient: freshChat, Message: "hi", LatestMessageID: &latest}
}

func TestFreshnessCheck(t *testing.T) {
	store := freshStore(t)
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	// No check requested.
	if r, err := store.checkReplyFresh(SendMessageRequest{Recipient: freshChat}); r != "" || err != nil {
		t.Fatalf("nil latest: %q %v", r, err)
	}
	// Empty chat, sender says so.
	if r, _ := store.checkReplyFresh(replyTo("")); r != "" {
		t.Fatalf("empty chat refused: %q", r)
	}

	addMsg(t, store, "A", t0, false)
	if r, _ := store.checkReplyFresh(replyTo("")); !strings.Contains(r, "list_messages") {
		t.Fatalf("claimed-empty chat with a message should be refused, got %q", r)
	}
	if r, _ := store.checkReplyFresh(replyTo("A")); r != "" {
		t.Fatalf("up to date refused: %q", r)
	}
	if r, _ := store.checkReplyFresh(replyTo("nope")); !strings.Contains(r, "is not a message in") {
		t.Fatalf("unknown id: %q", r)
	}

	// A new message in the same second still counts as newer.
	addMsg(t, store, "B", t0, false)
	r, _ := store.checkReplyFresh(replyTo("A"))
	if !strings.Contains(r, "1 new message(s)") || !strings.Contains(r, "list_messages") {
		t.Fatalf("stale not refused: %q", r)
	}

	// Our own API sends do not count; the owner's phone messages do.
	addMsg(t, store, "C", t0.Add(time.Second), true)
	if err := store.MarkSentViaAPI("C", freshChat); err != nil {
		t.Fatal(err)
	}
	if r, _ := store.checkReplyFresh(replyTo("B")); r != "" {
		t.Fatalf("own API send counted as new: %q", r)
	}
	addMsg(t, store, "D", t0.Add(2*time.Second), true)
	if r, _ := store.checkReplyFresh(replyTo("B")); r == "" {
		t.Fatal("message typed on the phone should count as new")
	}

	chat, ok, err := store.GetChat(freshChat, true)
	if err != nil || !ok || chat.LastMessageID == nil || *chat.LastMessageID != "D" {
		t.Fatalf("last_message_id: %+v %v %v", chat, ok, err)
	}
	if chat.LastMessage == nil || *chat.LastMessage != "text D" {
		t.Fatalf("last_message: %v", chat.LastMessage)
	}
	if chats, _ := store.ListChats(ListChatsParams{IncludeLastMessage: true}); len(chats) != 1 {
		t.Fatalf("chat listed %d times", len(chats))
	}
}

func TestStaleSendRefusedAtSubmission(t *testing.T) {
	store := freshStore(t)
	addMsg(t, store, "A", time.Now().Add(-time.Minute), false)
	addMsg(t, store, "B", time.Now(), false)

	sent := make(chan struct{}, 1)
	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) {
		sent <- struct{}{}
		return true, "sent"
	})
	q.checkFresh = store.checkReplyFresh
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/send", strings.NewReader(`{"recipient":"`+freshChat+`","message":"hi","latest_message_id":"A"}`))
	sendHandler(q, t.TempDir(), alwaysReady, false)(w, r)
	if w.Code != 409 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var resp SendMessageResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Success || !resp.Stale || resp.ID != "" {
		t.Fatalf("response %+v", resp)
	}
	select {
	case <-sent:
		t.Fatal("stale message was sent")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestStaleSendDroppedBeforeGoingOut(t *testing.T) {
	store := freshStore(t)
	addMsg(t, store, "A", time.Now().Add(-time.Minute), false)

	sent := false
	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) {
		sent = true
		return true, "sent"
	})
	q.checkFresh = store.checkReplyFresh

	job, _, ok := q.submit(replyTo("A"), func() {})
	if !ok {
		t.Fatal("not queued")
	}
	addMsg(t, store, "B", time.Now(), false) // arrives while queued

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)
	select {
	case res := <-job.done:
		if res.Success || sent || !strings.Contains(res.Message, "list_messages") {
			t.Fatalf("result %+v sent=%v", res, sent)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("job never finished")
	}
}

func TestLatestMessageIDSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := openSendQueueStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, req := range []SendMessageRequest{replyTo("A"), replyTo(""), {Recipient: freshChat, Message: "x"}} {
		job := &sendJob{id: string(rune('a' + i)), req: req}
		if err := s.insertJob(job, false, stageMain, time.Now(), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.loadQueued(stageMain)
	if err != nil || len(rows) != 3 {
		t.Fatalf("%v %d", err, len(rows))
	}
	got := map[string]*string{}
	for _, r := range rows {
		got[r.ID] = r.request().LatestMessageID
	}
	if got["a"] == nil || *got["a"] != "A" || got["b"] == nil || *got["b"] != "" || got["c"] != nil {
		t.Fatalf("round trip: %v %v %v", got["a"], got["b"], got["c"])
	}
}
