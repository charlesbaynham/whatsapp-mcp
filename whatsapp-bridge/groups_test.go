package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

type fakeGroupClient struct {
	registered map[string]bool
	created    *whatsmeow.ReqCreateGroup
	// failures maps participant user -> error code WhatsApp reports.
	failures  map[string]int
	createErr error
	topic     string
	linkCalls int
}

func (f *fakeGroupClient) IsOnWhatsApp(_ context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error) {
	var out []types.IsOnWhatsAppResponse
	for _, p := range phones {
		out = append(out, types.IsOnWhatsAppResponse{Query: "+" + p, JID: pnJID(p), IsIn: f.registered[p]})
	}
	return out, nil
}

func (f *fakeGroupClient) CreateGroup(_ context.Context, req whatsmeow.ReqCreateGroup) (*types.GroupInfo, error) {
	f.created = &req
	if f.createErr != nil {
		return nil, f.createErr
	}
	info := &types.GroupInfo{JID: types.JID{User: "1203630", Server: types.GroupServer}}
	for _, p := range req.Participants {
		info.Participants = append(info.Participants, types.GroupParticipant{JID: p, Error: f.failures[p.User]})
	}
	return info, nil
}

func (f *fakeGroupClient) SetGroupTopic(_ context.Context, _ types.JID, _, _, topic string) error {
	f.topic = topic
	return nil
}

func (f *fakeGroupClient) GetGroupInviteLink(context.Context, types.JID, bool) (string, error) {
	f.linkCalls++
	return "https://chat.whatsapp.com/abc", nil
}

func TestCreateGroupAddsParticipants(t *testing.T) {
	cli := &fakeGroupClient{registered: map[string]bool{"447700900001": true, "447700900002": true}}
	status, resp := createGroup(context.Background(), cli, fakeChats{}, nil, CreateGroupRequest{
		Name:         "Dinner",
		Participants: []string{"+447700900001", "447700900002@s.whatsapp.net"},
		Description:  "Friday",
	})
	if status != http.StatusOK || !resp.Success {
		t.Fatalf("got %d %+v", status, resp)
	}
	if resp.GroupJID != "1203630@g.us" || len(cli.created.Participants) != 2 {
		t.Fatalf("unexpected create: %+v / %+v", resp, cli.created)
	}
	for _, p := range resp.Participants {
		if !p.Added {
			t.Errorf("%s not added: %+v", p.Requested, p)
		}
	}
	if cli.topic != "Friday" {
		t.Errorf("description not set: %q", cli.topic)
	}
	if cli.linkCalls != 0 || resp.InviteLink != "" {
		t.Errorf("invite link fetched when everyone was added")
	}
}

func TestCreateGroupReportsPrivacyRefusalWithInviteLink(t *testing.T) {
	cli := &fakeGroupClient{
		registered: map[string]bool{"111": true, "222": true},
		failures:   map[string]int{"222": 403},
	}
	_, resp := createGroup(context.Background(), cli, fakeChats{}, nil, CreateGroupRequest{Name: "G", Participants: []string{"111", "222"}})
	if !resp.Success || resp.Participants[1].Added || resp.Participants[1].ErrorCode != 403 {
		t.Fatalf("unexpected: %+v", resp)
	}
	if resp.InviteLink == "" {
		t.Error("expected an invite link for the refused participant")
	}
}

func TestCreateGroupSkipsUnregisteredNumbers(t *testing.T) {
	cli := &fakeGroupClient{registered: map[string]bool{"111": true}}
	_, resp := createGroup(context.Background(), cli, fakeChats{}, nil, CreateGroupRequest{Name: "G", Participants: []string{"111", "999"}})
	if len(cli.created.Participants) != 1 || cli.created.Participants[0].User != "111" {
		t.Fatalf("unregistered number was submitted: %+v", cli.created.Participants)
	}
	if resp.Participants[1].Added || !strings.Contains(resp.Participants[1].Message, "not on WhatsApp") {
		t.Errorf("unexpected result for 999: %+v", resp.Participants[1])
	}
}

func TestCreateGroupRejectsBadInput(t *testing.T) {
	cli := &fakeGroupClient{registered: map[string]bool{"111": true}}
	for name, req := range map[string]CreateGroupRequest{
		"no name":         {Participants: []string{"111"}},
		"long name":       {Name: strings.Repeat("x", 26), Participants: []string{"111"}},
		"no participants": {Name: "G"},
		"bad number":      {Name: "G", Participants: []string{"12ab"}},
		"group as member": {Name: "G", Participants: []string{"123@g.us"}},
		"none registered": {Name: "G", Participants: []string{"999"}},
	} {
		cli.created = nil
		if status, resp := createGroup(context.Background(), cli, fakeChats{}, nil, req); status != http.StatusBadRequest || resp.Success {
			t.Errorf("%s: got %d %+v", name, status, resp)
		}
		if cli.created != nil {
			t.Errorf("%s: group was created", name)
		}
	}
}

func TestCreateGroupRefusedDuringReachoutTimelock(t *testing.T) {
	cli := &fakeGroupClient{registered: map[string]bool{"111": true}}
	lock := func() (bool, time.Time) { return true, time.Now().Add(time.Hour) }
	status, _ := createGroup(context.Background(), cli, fakeChats{}, lock, CreateGroupRequest{Name: "G", Participants: []string{"111"}})
	if status != http.StatusConflict || cli.created != nil {
		t.Fatalf("got %d, created=%v", status, cli.created != nil)
	}
}

func TestCreateGroupPropagatesCreateError(t *testing.T) {
	cli := &fakeGroupClient{registered: map[string]bool{"111": true}, createErr: errors.New("406")}
	status, resp := createGroup(context.Background(), cli, fakeChats{}, nil, CreateGroupRequest{Name: "G", Participants: []string{"111"}})
	if status != http.StatusBadGateway || resp.Success {
		t.Fatalf("got %d %+v", status, resp)
	}
}
