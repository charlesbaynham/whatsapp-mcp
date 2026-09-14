package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

func intPtr(n int) *int { return &n }

func TestPollRequestValidate(t *testing.T) {
	good := PollRequest{Question: "  Lunch? ", Options: []string{" Pizza", "Sushi "}}
	spec, err := good.validate()
	if err != nil {
		t.Fatal(err)
	}
	if spec.Question != "Lunch?" || spec.Options[0] != "Pizza" || spec.Options[1] != "Sushi" {
		t.Errorf("not trimmed: %+v", spec)
	}
	if spec.SelectableCount != 1 {
		t.Errorf("selectable_count defaults to %d, want 1 (single choice)", spec.SelectableCount)
	}
	any := PollRequest{Question: "q", Options: []string{"a", "b"}, SelectableCount: intPtr(0)}
	if spec, err := any.validate(); err != nil || spec.SelectableCount != 0 {
		t.Errorf("explicit 0 = any number: %+v %v", spec, err)
	}

	bad := map[string]PollRequest{
		"no question":   {Options: []string{"a", "b"}},
		"one option":    {Question: "q", Options: []string{"a"}},
		"empty option":  {Question: "q", Options: []string{"a", " "}},
		"duplicate":     {Question: "q", Options: []string{"a", "a "}},
		"too many":      {Question: "q", Options: strings.Split("a b c d e f g h i j k l m", " ")},
		"selectable>n":  {Question: "q", Options: []string{"a", "b"}, SelectableCount: intPtr(3)},
		"selectable<0":  {Question: "q", Options: []string{"a", "b"}, SelectableCount: intPtr(-1)},
		"long question": {Question: strings.Repeat("x", pollMaxQuestionLen+1), Options: []string{"a", "b"}},
	}
	for name, req := range bad {
		if _, err := req.validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestExtractPollFromEveryVariant(t *testing.T) {
	pc := &waProto.PollCreationMessage{
		Name: proto.String("Lunch?"),
		Options: []*waProto.PollCreationMessage_Option{
			{OptionName: proto.String("Pizza")}, {OptionName: proto.String("Sushi")},
		},
		SelectableOptionsCount: proto.Uint32(1),
	}
	variants := map[string]*waProto.Message{
		"v1": {PollCreationMessage: pc},
		"v2": {PollCreationMessageV2: pc},
		"v3": {PollCreationMessageV3: pc},
		"v4": {PollCreationMessageV4: &waProto.FutureProofMessage{Message: &waProto.Message{PollCreationMessageV3: pc}}},
		"v5": {PollCreationMessageV5: pc},
	}
	for name, msg := range variants {
		spec := extractPoll(msg)
		if spec == nil {
			t.Errorf("%s: no poll extracted", name)
			continue
		}
		if spec.Question != "Lunch?" || len(spec.Options) != 2 || spec.Options[1] != "Sushi" || spec.SelectableCount != 1 {
			t.Errorf("%s: %+v", name, spec)
		}
	}
	if extractPoll(&waProto.Message{Conversation: proto.String("hi")}) != nil {
		t.Error("text message extracted as a poll")
	}
	if extractPoll(nil) != nil {
		t.Error("nil message extracted as a poll")
	}
}

func TestResolvePollOptions(t *testing.T) {
	options := []string{"Pizza", "Sushi", "Salad"}
	hashes := whatsmeow.HashPollOptions(options)
	names, unmatched := resolvePollOptions(options, [][]byte{hashes[2], hashes[0], []byte("garbage")})
	if len(names) != 2 || names[0] != "Salad" || names[1] != "Pizza" || unmatched != 1 {
		t.Errorf("names=%v unmatched=%d", names, unmatched)
	}
	names, _ = resolvePollOptions(options, nil)
	if names == nil || len(names) != 0 {
		t.Errorf("a retracted vote must be an empty (not nil) selection: %#v", names)
	}
}

// pollFixture stores a poll from Alice in a group with a couple of votes.
func pollFixture(t *testing.T) (*MessageStore, PollSpec) {
	t.Helper()
	store := newTestStore(t)
	at := time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)
	spec := PollSpec{Question: "Lunch?", Options: []string{"Pizza", "Sushi", "Salad"}, SelectableCount: 1}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(store.StoreChat("g@g.us", "Team", at))
	must(store.StoreChat("447700900001@s.whatsapp.net", "Alice", at))
	must(store.StoreMessage("p1", "g@g.us", "447700900001", "Lunch?", at, false, mediaTypePoll, "", "", nil, nil, nil, 0))
	must(store.StorePoll("p1", "g@g.us", spec))
	must(store.StoreMessage("t1", "g@g.us", "447700900002", "hello", at.Add(time.Minute), false, "", "", "", nil, nil, nil, 0))
	must(store.RecordPollVote("g@g.us", "p1", "447700900001", []string{"Pizza"}, "v1", at.Add(2*time.Minute)))
	must(store.RecordPollVote("g@g.us", "p1", "447700900002", []string{"Sushi"}, "v2", at.Add(3*time.Minute)))
	return store, spec
}

func TestPollTallyAndVoteReplacement(t *testing.T) {
	store, spec := pollFixture(t)
	at := time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC)

	view, votes, err := store.pollView("g@g.us", "p1", spec, store.newSenderNamer())
	if err != nil {
		t.Fatal(err)
	}
	if view.TotalVoters != 2 || view.Results[0].Votes != 1 || view.Results[1].Votes != 1 || view.Results[2].Votes != 0 {
		t.Errorf("tally: %+v", view.Results)
	}
	if len(votes) != 2 || votes[0].VoterName != "Alice" || votes[0].Selected[0] != "Pizza" {
		t.Errorf("votes: %+v", votes)
	}
	if view.Results[0].Voters[0] != "Alice" {
		t.Errorf("voter names on the tally: %+v", view.Results[0])
	}

	// A newer vote replaces the voter's previous one.
	if err := store.RecordPollVote("g@g.us", "p1", "447700900001", []string{"Salad"}, "v3", at.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	view, _, _ = store.pollView("g@g.us", "p1", spec, nil)
	if view.Results[0].Votes != 0 || view.Results[2].Votes != 1 || view.TotalVoters != 2 {
		t.Errorf("after changing vote: %+v", view.Results)
	}

	// An older one, delivered late, does not.
	if err := store.RecordPollVote("g@g.us", "p1", "447700900001", []string{"Pizza"}, "v0", at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	view, _, _ = store.pollView("g@g.us", "p1", spec, nil)
	if view.Results[2].Votes != 1 {
		t.Errorf("a stale vote overwrote a newer one: %+v", view.Results)
	}

	// A retraction removes the voter from the count but keeps them listed.
	if err := store.RecordPollVote("g@g.us", "p1", "447700900002", nil, "v4", at.Add(6*time.Minute)); err != nil {
		t.Fatal(err)
	}
	view, votes, _ = store.pollView("g@g.us", "p1", spec, nil)
	if view.TotalVoters != 1 || view.Results[1].Votes != 0 || len(votes) != 2 {
		t.Errorf("after retraction: total=%d results=%+v votes=%d", view.TotalVoters, view.Results, len(votes))
	}
	// Empty selections must serialise as [] rather than null.
	body, _ := json.Marshal(votes[1])
	if !strings.Contains(string(body), `"selected":[]`) {
		t.Errorf("retracted vote json: %s", body)
	}
}

func TestFindPollFallsBackToIDAlone(t *testing.T) {
	store, _ := pollFixture(t)
	chat, spec, found, err := store.findPoll("g@g.us", "p1")
	if err != nil || !found || chat != "g@g.us" || spec.Question != "Lunch?" {
		t.Fatalf("exact: chat=%s spec=%+v found=%v err=%v", chat, spec, found, err)
	}
	// The vote's key can spell the chat differently (a LID); the id still finds it.
	chat, _, found, _ = store.findPoll("12345@lid", "p1")
	if !found || chat != "g@g.us" {
		t.Errorf("by id: chat=%s found=%v", chat, found)
	}
	_, _, found, _ = store.findPoll("g@g.us", "t1")
	if found {
		t.Error("a text message was found as a poll")
	}
}

func TestListMessagesCarriesPollOutcome(t *testing.T) {
	store, _ := pollFixture(t)
	msgs, err := store.ListMessages(ListMessagesParams{ChatJID: "g@g.us", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || msgs[1].ID != "p1" {
		t.Fatalf("messages: %+v", msgs)
	}
	if msgs[0].Poll != nil {
		t.Errorf("text message has a poll: %+v", msgs[0].Poll)
	}
	p := msgs[1].Poll
	if p == nil || p.Question != "Lunch?" || p.TotalVoters != 2 || p.Results[1].Votes != 1 || p.Results[1].Voters[0] != "447700900002" {
		t.Errorf("poll on message: %+v", p)
	}
	if msgs[1].MediaType != mediaTypePoll || msgs[1].Content != "Lunch?" {
		t.Errorf("poll message row: %+v", msgs[1])
	}

	polls, err := store.ListPolls("", 10, 0)
	if err != nil || len(polls) != 1 || polls[0].ID != "p1" {
		t.Errorf("ListPolls: %+v %v", polls, err)
	}
	none, _ := store.ListPolls("a@s.whatsapp.net", 10, 0)
	if len(none) != 0 {
		t.Errorf("ListPolls for another chat: %+v", none)
	}
}

func TestPollRoutes(t *testing.T) {
	store, _ := pollFixture(t)
	mux := http.NewServeMux()
	registerPollRoutes(mux, store)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/polls/g@g.us/p1", nil))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var res PollResultsView
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if res.MessageID != "p1" || res.ChatName != "Team" || res.SenderName != "Alice" || res.TotalVoters != 2 || len(res.Votes) != 2 {
		t.Errorf("results: %+v", res)
	}
	if res.Votes[0].VoterName != "Alice" || res.Votes[1].Selected[0] != "Sushi" {
		t.Errorf("votes: %+v", res.Votes)
	}

	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/polls/g@g.us/t1", nil))
	if w.Code != 404 {
		t.Errorf("text message as poll: status %d", w.Code)
	}

	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/polls?chat_jid=g@g.us", nil))
	var list []MessageView
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list) != 1 || list[0].Poll == nil {
		t.Errorf("list: %d %s %v", w.Code, w.Body, err)
	}
}

func TestSendHandlerValidatesPolls(t *testing.T) {
	store := t.TempDir()
	post := func(body string) (*httptest.ResponseRecorder, chan *PollSpec) {
		got := make(chan *PollSpec, 1)
		r := httptest.NewRequest("POST", "/api/send", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w, _ := serveSend(t, store, r, func(_ context.Context, req SendMessageRequest) (bool, string) {
			got <- req.poll
			return true, "sent"
		})
		return w, got
	}

	w, got := post(`{"recipient":"g@g.us","poll":{"question":"Lunch?","options":["Pizza","Sushi"]}}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("valid poll: status %d: %s", w.Code, w.Body)
	}
	select {
	case spec := <-got:
		if spec == nil || spec.Question != "Lunch?" || spec.SelectableCount != 1 {
			t.Errorf("poll reaching the sender: %+v", spec)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the send never ran")
	}

	for name, body := range map[string]string{
		"one option": `{"recipient":"g@g.us","poll":{"question":"Lunch?","options":["Pizza"]}}`,
		"with media": `{"recipient":"g@g.us","media_path":"x.jpg","poll":{"question":"q","options":["a","b"]}}`,
		"nothing":    `{"recipient":"g@g.us"}`,
	} {
		w, _ := post(body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, w.Code)
		}
	}
}

func TestPollVoteEventAndWebhookText(t *testing.T) {
	store := newTestStore(t)
	pub := testPublisher(store)
	vote := WebhookEvent{
		MessageID: "v1", ChatJID: "g@g.us", ChatName: "Team", Sender: "447700900001", SenderName: "Alice",
		Timestamp: "2026-09-14T10:02:00Z",
		PollVote: &PollVotePayload{PollID: "p1", Question: "Lunch?", Selected: []string{"Pizza"},
			Results: []PollOptionResult{{Option: "Pizza", Votes: 1}, {Option: "Sushi", Votes: 0}}, TotalVoters: 1},
	}
	pub.PublishPollVote(vote)
	evs, _ := store.ListEvents(0, 10)
	if len(evs) != 1 || evs[0].Type != eventPollVote || evs[0].ChatJID != "g@g.us" {
		t.Fatalf("events: %+v", evs)
	}
	var back WebhookEvent
	if err := json.Unmarshal(evs[0].Data, &back); err != nil || back.PollVote == nil || back.PollVote.Selected[0] != "Pizza" {
		t.Errorf("payload: %s %v", evs[0].Data, err)
	}

	text := buildWebhookText([]WebhookEvent{vote})
	if !strings.Contains(text, `447700900001: voted "Pizza" on poll "Lunch?" — now Pizza 1, Sushi 0 (1 voter)`) {
		t.Errorf("vote text:\n%s", text)
	}
	poll := WebhookEvent{MessageID: "p1", ChatJID: "g@g.us", ChatName: "Team", Sender: "447700900001", Content: "Lunch?",
		MediaType: mediaTypePoll, Timestamp: "2026-09-14T10:00:00Z",
		Poll: &PollView{Question: "Lunch?", Options: []string{"Pizza", "Sushi"}, SelectableCount: 0}}
	text = buildWebhookText([]WebhookEvent{poll})
	if !strings.Contains(text, "[poll] Lunch? — options: Pizza / Sushi (pick any number)") {
		t.Errorf("poll text:\n%s", text)
	}
	retract := vote
	retract.PollVote = &PollVotePayload{Question: "Lunch?", Selected: []string{}, Results: poll.Poll.Results}
	if !strings.Contains(describePollVote(retract.PollVote), "withdrew their vote") {
		t.Errorf("retraction text: %s", describePollVote(retract.PollVote))
	}
}
