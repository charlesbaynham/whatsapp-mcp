package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// WhatsApp rejects longer group names with a 406.
const maxGroupNameLen = 25

// groupClient is the slice of *whatsmeow.Client that group creation needs.
type groupClient interface {
	registrationChecker
	CreateGroup(ctx context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error)
	SetGroupTopic(ctx context.Context, jid types.JID, previousID, newID, topic string) error
	GetGroupInviteLink(ctx context.Context, jid types.JID, reset bool) (string, error)
}

// CreateGroupRequest is the body of POST /api/groups.
type CreateGroupRequest struct {
	Name string `json:"name"`
	// Participants are bare phone numbers (an optional leading "+" is
	// dropped) or full user JIDs. The bot itself is added implicitly.
	Participants []string `json:"participants"`
	Description  string   `json:"description,omitempty"`
}

// GroupParticipantResult is the outcome of adding one requested participant.
type GroupParticipantResult struct {
	Requested string `json:"requested"`
	JID       string `json:"jid,omitempty"`
	Added     bool   `json:"added"`
	ErrorCode int    `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

// CreateGroupResponse is what POST /api/groups answers with.
type CreateGroupResponse struct {
	Success      bool                     `json:"success"`
	Message      string                   `json:"message"`
	GroupJID     string                   `json:"group_jid,omitempty"`
	Name         string                   `json:"name,omitempty"`
	Participants []GroupParticipantResult `json:"participants,omitempty"`
	// InviteLink is fetched only when someone could not be added directly,
	// so they can be sent it instead.
	InviteLink string   `json:"invite_link,omitempty"`
	Warnings   []string `json:"warnings,omitempty"`
}

// parseParticipant turns one requested participant into a user JID.
func parseParticipant(s string) (types.JID, error) {
	s = strings.TrimSpace(s)
	if !strings.Contains(s, "@") {
		s = strings.TrimPrefix(s, "+")
		if s == "" || strings.Trim(s, "0123456789") != "" {
			return types.JID{}, fmt.Errorf("not a phone number or JID: %q", s)
		}
		return types.JID{User: s, Server: types.DefaultUserServer}, nil
	}
	jid, err := types.ParseJID(s)
	if err != nil || jid.User == "" {
		return types.JID{}, fmt.Errorf("invalid JID: %q", s)
	}
	if jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer {
		return types.JID{}, fmt.Errorf("not a user JID: %q", s)
	}
	return jid.ToNonAD(), nil
}

// participantErrorText explains the per-participant codes WhatsApp returns
// when a group is created.
func participantErrorText(code int) string {
	switch code {
	case 403:
		return "their privacy settings do not allow being added; send them the invite link"
	case 408:
		return "they recently left this group and cannot be re-added yet"
	case 409:
		return "already a member"
	case 401:
		return "they have blocked this account"
	case 404:
		return "not on WhatsApp"
	default:
		return fmt.Sprintf("WhatsApp refused to add them (code %d)", code)
	}
}

// createGroup validates req, checks phone-number participants are on
// WhatsApp, and creates the group owned by this account. It returns the HTTP
// status to answer with alongside the response.
func createGroup(ctx context.Context, cli groupClient, chats chatExistenceChecker, lock func() (bool, time.Time), req CreateGroupRequest) (int, CreateGroupResponse) {
	fail := func(status int, format string, args ...any) (int, CreateGroupResponse) {
		return status, CreateGroupResponse{Message: fmt.Sprintf(format, args...)}
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		return fail(http.StatusBadRequest, "name is required")
	}
	if n := utf8.RuneCountInString(name); n > maxGroupNameLen {
		return fail(http.StatusBadRequest, "name is %d characters; WhatsApp allows at most %d", n, maxGroupNameLen)
	}
	if len(req.Participants) == 0 {
		return fail(http.StatusBadRequest, "at least one participant is required")
	}
	if lock != nil {
		if active, ends := lock(); active {
			return fail(http.StatusConflict, "WhatsApp's reach-out time-lock is active until %s; adding people to a new group now would fail", ends.Format(time.RFC3339))
		}
	}

	results := make([]GroupParticipantResult, len(req.Participants))
	var jids []types.JID
	index := map[types.JID]int{}
	for i, raw := range req.Participants {
		results[i].Requested = raw
		jid, err := parseParticipant(raw)
		if err != nil {
			return fail(http.StatusBadRequest, "participant %d: %v", i, err)
		}
		// Same check as a first message: adding a number that is not on
		// WhatsApp still costs a reach-out.
		jid, err = verifyRecipientRegistered(ctx, cli, chats, jid)
		if err != nil {
			results[i].Message = err.Error()
			continue
		}
		results[i].JID = jid.String()
		if _, dup := index[jid]; dup {
			results[i].Message = "duplicate of an earlier participant"
			continue
		}
		index[jid] = i
		jids = append(jids, jid)
	}
	if len(jids) == 0 {
		return http.StatusBadRequest, CreateGroupResponse{Message: "no participant could be added", Participants: results}
	}

	info, err := cli.CreateGroup(ctx, whatsmeow.ReqCreateGroup{Name: name, Participants: jids})
	if err != nil {
		return http.StatusBadGateway, CreateGroupResponse{Message: fmt.Sprintf("failed to create group: %v", err), Participants: results}
	}

	// Match what WhatsApp reports back to what was asked for. A participant
	// may come back under their LID or phone number, so try both.
	for _, p := range info.Participants {
		i, ok := -1, false
		for _, cand := range []types.JID{p.JID, p.PhoneNumber, p.LID} {
			if cand.IsEmpty() {
				continue
			}
			if i, ok = index[cand.ToNonAD()]; ok {
				break
			}
		}
		if !ok {
			continue
		}
		results[i].ErrorCode = p.Error
		results[i].Added = p.Error == 0
		if p.Error != 0 {
			results[i].Message = participantErrorText(p.Error)
		}
	}
	// Anyone submitted but not reported is assumed added: WhatsApp lists
	// only failures for some participants.
	needInvite := false
	for _, i := range index {
		r := &results[i]
		if r.ErrorCode == 0 && r.Message == "" {
			r.Added = true
		}
		if !r.Added {
			needInvite = true
		}
	}

	resp := CreateGroupResponse{
		Success:      true,
		GroupJID:     info.JID.String(),
		Name:         name,
		Participants: results,
	}
	if desc := strings.TrimSpace(req.Description); desc != "" {
		if err := cli.SetGroupTopic(ctx, info.JID, "", "", desc); err != nil {
			resp.Warnings = append(resp.Warnings, fmt.Sprintf("group created but setting the description failed: %v", err))
		}
	}
	if needInvite {
		link, err := cli.GetGroupInviteLink(ctx, info.JID, false)
		if err != nil {
			resp.Warnings = append(resp.Warnings, fmt.Sprintf("could not fetch an invite link: %v", err))
		} else {
			resp.InviteLink = link
		}
	}
	added := 0
	for _, r := range results {
		if r.Added {
			added++
		}
	}
	resp.Message = fmt.Sprintf("created group %q with %d of %d requested participant(s) added", name, added, len(results))
	return http.StatusOK, resp
}

func registerGroupRoutes(mux *http.ServeMux, cli groupClient, store *MessageStore, lock func() (bool, time.Time), ready func(http.ResponseWriter) bool) {
	mux.HandleFunc("/api/groups", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if ready != nil && !ready(w) {
			return
		}
		var req CreateGroupRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}
		var chats chatExistenceChecker
		if store != nil {
			chats = store
		}
		status, resp := createGroup(r.Context(), cli, chats, lock, req)
		if resp.Success && store != nil {
			// So the group is listed and addressable straight away, before
			// any message has been exchanged in it.
			if err := store.StoreChat(resp.GroupJID, resp.Name, time.Now()); err != nil {
				resp.Warnings = append(resp.Warnings, fmt.Sprintf("group created but not recorded locally: %v", err))
			}
		}
		writeJSON(w, status, resp)
	})
}
