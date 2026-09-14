package main

// Polls: sending a poll creation message, storing polls seen in any chat,
// decrypting the votes that come back, and reading the outcome.
//
// A poll is stored as an ordinary message row (media_type "poll", content =
// the question) plus a JSON blob of its options in messages.poll. Votes live
// in poll_votes, one row per (poll, voter): WhatsApp treats each vote as a
// replacement for the voter's previous one, and an empty vote as a
// retraction, so the table always holds the current state and tallies are
// computed from it on read.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	mediaTypePoll = "poll"

	pollMinOptions     = 2
	pollMaxOptions     = 12
	pollMaxQuestionLen = 255
	pollMaxOptionLen   = 100
)

// PollRequest is the poll part of POST /api/send. selectable_count is a
// pointer so that leaving it out means "single choice" rather than "any
// number", which is what a zero would mean on the wire.
type PollRequest struct {
	Question        string   `json:"question"`
	Options         []string `json:"options"`
	SelectableCount *int     `json:"selectable_count,omitempty"`
}

// PollSpec is a poll as sent and as stored with its message.
type PollSpec struct {
	Question string   `json:"question"`
	Options  []string `json:"options"`
	// SelectableCount is how many options a voter may pick: 1 for a single
	// choice, 0 for any number.
	SelectableCount int `json:"selectable_count"`
}

// validate normalises the request into a PollSpec, or explains why it can't.
func (p *PollRequest) validate() (PollSpec, error) {
	spec := PollSpec{Question: strings.TrimSpace(p.Question), SelectableCount: 1}
	if spec.Question == "" {
		return spec, fmt.Errorf("poll question is required")
	}
	if len(spec.Question) > pollMaxQuestionLen {
		return spec, fmt.Errorf("poll question is longer than %d characters", pollMaxQuestionLen)
	}
	if len(p.Options) < pollMinOptions || len(p.Options) > pollMaxOptions {
		return spec, fmt.Errorf("a poll needs between %d and %d options, got %d", pollMinOptions, pollMaxOptions, len(p.Options))
	}
	seen := map[string]bool{}
	for i, o := range p.Options {
		o = strings.TrimSpace(o)
		if o == "" {
			return spec, fmt.Errorf("poll option %d is empty", i+1)
		}
		if len(o) > pollMaxOptionLen {
			return spec, fmt.Errorf("poll option %d is longer than %d characters", i+1, pollMaxOptionLen)
		}
		// Votes come back as a hash of the option text, so two identical
		// options would be indistinguishable.
		if seen[o] {
			return spec, fmt.Errorf("poll option %q appears more than once", o)
		}
		seen[o] = true
		spec.Options = append(spec.Options, o)
	}
	if p.SelectableCount != nil {
		if *p.SelectableCount < 0 || *p.SelectableCount > len(spec.Options) {
			return spec, fmt.Errorf("selectable_count must be between 0 (any number) and %d", len(spec.Options))
		}
		spec.SelectableCount = *p.SelectableCount
	}
	return spec, nil
}

// buildPollMessage is the WhatsApp message for a poll.
func buildPollMessage(client *whatsmeow.Client, spec PollSpec) *waProto.Message {
	return client.BuildPollCreation(spec.Question, spec.Options, spec.SelectableCount)
}

// extractPoll returns the poll a message creates, or nil.
func extractPoll(msg *waProto.Message) *PollSpec {
	if msg == nil {
		return nil
	}
	pc := pollCreation(msg)
	if pc == nil {
		// V4 wraps the real thing in a FutureProofMessage.
		pc = pollCreation(msg.GetPollCreationMessageV4().GetMessage())
	}
	if pc == nil {
		return nil
	}
	spec := &PollSpec{Question: pc.GetName(), SelectableCount: int(pc.GetSelectableOptionsCount())}
	for _, o := range pc.GetOptions() {
		spec.Options = append(spec.Options, o.GetOptionName())
	}
	return spec
}

func pollCreation(msg *waProto.Message) *waProto.PollCreationMessage {
	if msg == nil {
		return nil
	}
	for _, pc := range []*waProto.PollCreationMessage{
		msg.GetPollCreationMessage(), msg.GetPollCreationMessageV2(), msg.GetPollCreationMessageV3(),
		msg.GetPollCreationMessageV5(), msg.GetPollCreationMessageV6(),
	} {
		if pc != nil {
			return pc
		}
	}
	return nil
}

// --- storage ---

func createPollTables(db *sql.DB) error {
	if err := ensureColumn(db, "messages", "poll", "TEXT"); err != nil {
		return fmt.Errorf("failed to add messages.poll: %v", err)
	}
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS poll_votes (
			chat_jid TEXT NOT NULL,
			poll_id TEXT NOT NULL,
			voter TEXT NOT NULL,
			selected TEXT NOT NULL,
			vote_id TEXT NOT NULL DEFAULT '',
			voted_at TIMESTAMP NOT NULL,
			PRIMARY KEY (chat_jid, poll_id, voter)
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create poll_votes table: %v", err)
	}
	return nil
}

// StorePoll attaches a poll's options to an already stored message.
func (store *MessageStore) StorePoll(id, chatJID string, spec PollSpec) error {
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	_, err = store.db.Exec(`UPDATE messages SET poll = ? WHERE id = ? AND chat_jid = ?`, string(data), id, chatJID)
	return err
}

// findPoll looks a poll up by message id, in the given chat first. A vote's
// poll key can spell the chat differently from how the poll was stored
// (phone number vs LID), so the id alone is the fallback.
func (store *MessageStore) findPoll(chatJID, pollID string) (string, PollSpec, bool, error) {
	var storedChat, raw string
	err := store.db.QueryRow(
		`SELECT chat_jid, poll FROM messages WHERE id = ? AND poll IS NOT NULL AND poll != ''
		 ORDER BY CASE WHEN chat_jid = ? THEN 0 ELSE 1 END LIMIT 1`, pollID, chatJID,
	).Scan(&storedChat, &raw)
	if err == sql.ErrNoRows {
		return "", PollSpec{}, false, nil
	}
	if err != nil {
		return "", PollSpec{}, false, err
	}
	var spec PollSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return "", PollSpec{}, false, fmt.Errorf("poll %s has a corrupt options record: %v", pollID, err)
	}
	return storedChat, spec, true, nil
}

// RecordPollVote stores a voter's current selection, replacing an earlier one
// unless it is newer than this: votes can be delivered out of order.
func (store *MessageStore) RecordPollVote(chatJID, pollID, voter string, selected []string, voteID string, votedAt time.Time) error {
	if selected == nil {
		selected = []string{}
	}
	data, err := json.Marshal(selected)
	if err != nil {
		return err
	}
	_, err = store.db.Exec(
		`INSERT INTO poll_votes (chat_jid, poll_id, voter, selected, vote_id, voted_at) VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(chat_jid, poll_id, voter) DO UPDATE SET
		   selected = excluded.selected, vote_id = excluded.vote_id, voted_at = excluded.voted_at
		 WHERE julianday(excluded.voted_at) >= julianday(poll_votes.voted_at)`,
		chatJID, pollID, voter, string(data), voteID, votedAt,
	)
	return err
}

// PollVoteView is one voter's current selection.
type PollVoteView struct {
	Voter     string    `json:"voter"`
	VoterName string    `json:"voter_name"`
	Selected  []string  `json:"selected"`
	Timestamp time.Time `json:"timestamp"`
}

// PollOptionResult is the tally for one option, in the poll's option order.
type PollOptionResult struct {
	Option string   `json:"option"`
	Votes  int      `json:"votes"`
	Voters []string `json:"voters"`
}

// PollView is a poll and its current outcome, as attached to a message.
type PollView struct {
	Question        string             `json:"question"`
	Options         []string           `json:"options"`
	SelectableCount int                `json:"selectable_count"`
	Results         []PollOptionResult `json:"results"`
	TotalVoters     int                `json:"total_voters"`
}

// PollResultsView is GET /api/polls/{chat_jid}/{message_id}: the poll, who
// asked it, and every vote.
type PollResultsView struct {
	MessageID  string    `json:"message_id"`
	ChatJID    string    `json:"chat_jid"`
	ChatName   string    `json:"chat_name"`
	Sender     string    `json:"sender"`
	SenderName string    `json:"sender_name"`
	IsFromMe   bool      `json:"is_from_me"`
	Timestamp  time.Time `json:"timestamp"`
	PollView
	Votes []PollVoteView `json:"votes"`
}

// pollVotes lists the current vote of everyone who has voted, oldest first.
// A retracted vote (empty selection) is kept so the voter is still listed.
func (store *MessageStore) pollVotes(chatJID, pollID string, namer *senderNamer) ([]PollVoteView, error) {
	rows, err := store.db.Query(
		`SELECT voter, selected, voted_at FROM poll_votes WHERE chat_jid = ? AND poll_id = ? ORDER BY julianday(voted_at) ASC, voter`,
		chatJID, pollID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PollVoteView{}
	for rows.Next() {
		var v PollVoteView
		var raw string
		var ts sql.NullTime
		if err := rows.Scan(&v.Voter, &raw, &ts); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(raw), &v.Selected); err != nil || v.Selected == nil {
			v.Selected = []string{}
		}
		if ts.Valid {
			v.Timestamp = ts.Time
		}
		if namer != nil {
			v.VoterName = namer.name(v.Voter, namer.isMe(v.Voter))
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// pollView tallies the votes for a poll.
func (store *MessageStore) pollView(chatJID, pollID string, spec PollSpec, namer *senderNamer) (PollView, []PollVoteView, error) {
	votes, err := store.pollVotes(chatJID, pollID, namer)
	if err != nil {
		return PollView{}, nil, err
	}
	return tallyPoll(spec, votes), votes, nil
}

func tallyPoll(spec PollSpec, votes []PollVoteView) PollView {
	view := PollView{Question: spec.Question, Options: spec.Options, SelectableCount: spec.SelectableCount}
	if view.Options == nil {
		view.Options = []string{}
	}
	index := map[string]int{}
	for i, o := range spec.Options {
		view.Results = append(view.Results, PollOptionResult{Option: o, Voters: []string{}})
		index[o] = i
	}
	if view.Results == nil {
		view.Results = []PollOptionResult{}
	}
	for _, v := range votes {
		if len(v.Selected) == 0 {
			continue
		}
		view.TotalVoters++
		name := v.VoterName
		if name == "" {
			name = v.Voter
		}
		for _, o := range v.Selected {
			if i, ok := index[o]; ok {
				view.Results[i].Votes++
				view.Results[i].Voters = append(view.Results[i].Voters, name)
			}
		}
	}
	return view
}

// fillPolls attaches the current outcome to every poll message in msgs.
func (store *MessageStore) fillPolls(msgs []MessageView) error {
	var namer *senderNamer
	for i := range msgs {
		if msgs[i].Poll == nil {
			continue
		}
		if namer == nil {
			namer = store.newSenderNamer()
		}
		spec := PollSpec{Question: msgs[i].Poll.Question, Options: msgs[i].Poll.Options, SelectableCount: msgs[i].Poll.SelectableCount}
		view, _, err := store.pollView(msgs[i].ChatJID, msgs[i].ID, spec, namer)
		if err != nil {
			return err
		}
		msgs[i].Poll = &view
	}
	return nil
}

// GetPollResults returns a poll message with every vote on it.
func (store *MessageStore) GetPollResults(chatJID, messageID string) (PollResultsView, bool, error) {
	found, err := store.queryMessages("m.chat_jid = ? AND m.id = ? AND m.media_type = ?", "LIMIT 1", chatJID, messageID, mediaTypePoll)
	if err != nil || len(found) == 0 || found[0].Poll == nil {
		return PollResultsView{}, false, err
	}
	m := found[0]
	namer := store.newSenderNamer()
	spec := PollSpec{Question: m.Poll.Question, Options: m.Poll.Options, SelectableCount: m.Poll.SelectableCount}
	view, votes, err := store.pollView(chatJID, messageID, spec, namer)
	if err != nil {
		return PollResultsView{}, false, err
	}
	return PollResultsView{
		MessageID: m.ID, ChatJID: m.ChatJID, ChatName: m.ChatName,
		Sender: m.Sender, SenderName: namer.name(m.Sender, m.IsFromMe),
		IsFromMe: m.IsFromMe, Timestamp: m.Timestamp,
		PollView: view, Votes: votes,
	}, true, nil
}

// ListPolls returns poll messages newest first, optionally in one chat.
func (store *MessageStore) ListPolls(chatJID string, limit, page int) ([]MessageView, error) {
	where := "m.media_type = ?"
	args := []any{mediaTypePoll}
	if chatJID != "" {
		where += " AND m.chat_jid = ?"
		args = append(args, chatJID)
	}
	limit, offset := clampPage(limit, page)
	args = append(args, limit, offset)
	msgs, err := store.queryMessages(where, "ORDER BY julianday(m.timestamp) DESC LIMIT ? OFFSET ?", args...)
	if err != nil {
		return nil, err
	}
	store.newSenderNamer().fill(msgs)
	return msgs, nil
}

// --- receiving votes ---

// PollVotePayload is what a poll.vote event carries beyond the usual
// message envelope: which poll, what the voter now selects, and the tally.
type PollVotePayload struct {
	PollID   string   `json:"poll_id"`
	Question string   `json:"question"`
	Selected []string `json:"selected"`
	// Results is the whole poll's current tally, not just this vote.
	Results     []PollOptionResult `json:"results"`
	TotalVoters int                `json:"total_voters"`
}

// resolvePollOptions maps the option hashes in a vote back to option text.
// Unmatched hashes (an option the stored poll doesn't know) are dropped.
func resolvePollOptions(options []string, selected [][]byte) (names []string, unmatched int) {
	hashes := whatsmeow.HashPollOptions(options)
	names = []string{}
	for _, sel := range selected {
		matched := false
		for i, h := range hashes {
			if bytes.Equal(h, sel) {
				names = append(names, options[i])
				matched = true
				break
			}
		}
		if !matched {
			unmatched++
		}
	}
	return names, unmatched
}

// handlePollVote decrypts an incoming vote, records it against its poll and
// publishes the poll's new tally. Nothing is stored in messages: a vote is
// an update to the poll, not a message of its own.
func handlePollVote(ctx context.Context, client *whatsmeow.Client, store *MessageStore, pub *Publisher, msg *events.Message, chatJID, chatName, sender string, logger waLog.Logger) {
	update := msg.Message.GetPollUpdateMessage()
	pollID := update.GetPollCreationMessageKey().GetID()

	vote, err := client.DecryptPollVote(ctx, msg)
	if err != nil {
		logger.Warnf("Failed to decrypt vote %s on poll %s in %s: %v", msg.Info.ID, pollID, chatJID, err)
		return
	}
	pollChat, spec, found, err := store.findPoll(chatJID, pollID)
	if err != nil {
		logger.Warnf("Failed to look up poll %s for vote %s: %v", pollID, msg.Info.ID, err)
		return
	}
	if !found {
		logger.Warnf("Vote %s in %s is for poll %s, which is not in the store", msg.Info.ID, chatJID, pollID)
		return
	}
	selected, unmatched := resolvePollOptions(spec.Options, vote.GetSelectedOptions())
	if unmatched > 0 {
		logger.Warnf("Vote %s on poll %s: %d selected option(s) match none of the stored options", msg.Info.ID, pollID, unmatched)
	}

	votedAt := msg.Info.Timestamp
	if ms := update.GetSenderTimestampMS(); ms > 0 {
		votedAt = time.UnixMilli(ms)
	}
	if err := store.RecordPollVote(pollChat, pollID, sender, selected, msg.Info.ID, votedAt); err != nil {
		logger.Warnf("Failed to record vote %s on poll %s: %v", msg.Info.ID, pollID, err)
		return
	}

	namer := store.newSenderNamer()
	view, _, err := store.pollView(pollChat, pollID, spec, namer)
	if err != nil {
		logger.Warnf("Failed to tally poll %s: %v", pollID, err)
		return
	}
	if pub != nil {
		pub.PublishPollVote(WebhookEvent{
			MessageID:  msg.Info.ID,
			ChatJID:    pollChat,
			ChatName:   chatName,
			Sender:     sender,
			SenderName: namer.name(sender, msg.Info.IsFromMe),
			Timestamp:  msg.Info.Timestamp.Format(time.RFC3339),
			IsFromMe:   msg.Info.IsFromMe,
			PollVote: &PollVotePayload{
				PollID: pollID, Question: spec.Question, Selected: selected,
				Results: view.Results, TotalVoters: view.TotalVoters,
			},
		})
	}
	direction := "←"
	if msg.Info.IsFromMe {
		direction = "→"
	}
	fmt.Printf("[%s] %s %s (%s): [poll vote on %s: %d option(s) selected]\n",
		msg.Info.Timestamp.Format("2006-01-02 15:04:05"), direction, sender, pollChat, pollID, len(selected))
}

// describePoll is the one-line rendering of a poll for text consumers.
func describePoll(p *PollView) string {
	if p == nil {
		return ""
	}
	var b strings.Builder
	fmt.Fprintf(&b, "[poll] %s — options: %s", p.Question, strings.Join(p.Options, " / "))
	if p.SelectableCount == 0 {
		b.WriteString(" (pick any number)")
	}
	if p.TotalVoters > 0 {
		b.WriteString(" — " + describePollResults(p.Results, p.TotalVoters))
	}
	return b.String()
}

func describePollResults(results []PollOptionResult, totalVoters int) string {
	parts := make([]string, 0, len(results))
	for _, r := range results {
		parts = append(parts, fmt.Sprintf("%s %d", r.Option, r.Votes))
	}
	voters := "voters"
	if totalVoters == 1 {
		voters = "voter"
	}
	return fmt.Sprintf("%s (%d %s)", strings.Join(parts, ", "), totalVoters, voters)
}

// describePollVote is the one-line rendering of a vote for text consumers.
func describePollVote(v *PollVotePayload) string {
	if v == nil {
		return ""
	}
	if len(v.Selected) == 0 {
		return fmt.Sprintf("withdrew their vote on poll %q — now %s", v.Question, describePollResults(v.Results, v.TotalVoters))
	}
	quoted := make([]string, len(v.Selected))
	for i, s := range v.Selected {
		quoted[i] = fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("voted %s on poll %q — now %s", strings.Join(quoted, ", "), v.Question, describePollResults(v.Results, v.TotalVoters))
}

// --- REST ---

func registerPollRoutes(mux *http.ServeMux, store *MessageStore) {
	mux.HandleFunc("/api/polls", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		limit, err := queryInt(r, "limit", readDefaultLimit)
		if err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		page, err := queryInt(r, "page", 0)
		if err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		polls, err := store.ListPolls(r.URL.Query().Get("chat_jid"), limit, page)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list polls: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, polls)
	})

	mux.HandleFunc("/api/polls/{chat_jid}/{message_id}", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		res, found, err := store.GetPollResults(r.PathValue("chat_jid"), r.PathValue("message_id"))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to look up poll: %v", err)
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "no poll %q in chat %q", r.PathValue("message_id"), r.PathValue("chat_jid"))
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
}
