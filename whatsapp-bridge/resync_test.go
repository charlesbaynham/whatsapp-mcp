package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func TestParseResyncRequest(t *testing.T) {
	valid := ResyncRequest{
		ChatJID:              "447700900000@s.whatsapp.net",
		OldestMessageID:      "3EB0ABCDEF",
		OldestMessageFromMe:  true,
		OldestMessageUnixSec: 1700000000,
	}

	t.Run("valid request defaults the count", func(t *testing.T) {
		info, count, err := parseResyncRequest(valid)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != defaultResyncCount {
			t.Fatalf("count = %d, want default %d", count, defaultResyncCount)
		}
		wantChat := jid("447700900000", types.DefaultUserServer)
		if info.Chat != wantChat {
			t.Fatalf("Chat = %v, want %v", info.Chat, wantChat)
		}
		if !info.IsFromMe {
			t.Fatalf("IsFromMe = false, want true")
		}
		if info.ID != valid.OldestMessageID {
			t.Fatalf("ID = %q, want %q", info.ID, valid.OldestMessageID)
		}
		if !info.Timestamp.Equal(time.Unix(valid.OldestMessageUnixSec, 0)) {
			t.Fatalf("Timestamp = %v, want %v", info.Timestamp, time.Unix(valid.OldestMessageUnixSec, 0))
		}
	})

	t.Run("explicit count is kept", func(t *testing.T) {
		req := valid
		req.Count = 10
		_, count, err := parseResyncRequest(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 10 {
			t.Fatalf("count = %d, want 10", count)
		}
	})

	cases := []struct {
		name string
		mut  func(ResyncRequest) ResyncRequest
	}{
		{"missing chat_jid", func(r ResyncRequest) ResyncRequest { r.ChatJID = ""; return r }},
		{"unparseable chat_jid", func(r ResyncRequest) ResyncRequest { r.ChatJID = "not a jid"; return r }},
		{"missing oldest_message_id", func(r ResyncRequest) ResyncRequest { r.OldestMessageID = ""; return r }},
		{"missing timestamp", func(r ResyncRequest) ResyncRequest { r.OldestMessageUnixSec = 0; return r }},
		{"negative timestamp", func(r ResyncRequest) ResyncRequest { r.OldestMessageUnixSec = -1; return r }},
		{"zero count after explicit set is still the default, not rejected", func(r ResyncRequest) ResyncRequest { r.Count = 0; return r }},
		{"negative count", func(r ResyncRequest) ResyncRequest { r.Count = -5; return r }},
		{"count above the ceiling", func(r ResyncRequest) ResyncRequest { r.Count = maxResyncCount + 1; return r }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := c.mut(valid)
			_, _, err := parseResyncRequest(req)
			wantErr := c.name != "zero count after explicit set is still the default, not rejected"
			if wantErr && err == nil {
				t.Fatalf("expected an error, got none")
			}
			if !wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}

	t.Run("count at the ceiling is accepted", func(t *testing.T) {
		req := valid
		req.Count = maxResyncCount
		_, count, err := parseResyncRequest(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != maxResyncCount {
			t.Fatalf("count = %d, want %d", count, maxResyncCount)
		}
	})
}
