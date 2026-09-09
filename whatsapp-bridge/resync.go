package main

import (
	"fmt"
	"time"

	"go.mau.fi/whatsmeow/types"
)

// defaultResyncCount mirrors whatsmeow's own doc comment on
// BuildHistorySyncRequest: "the recommended number of messages to request at
// a time is 50."
const defaultResyncCount = 50

// maxResyncCount is not a WhatsApp protocol limit — whatsmeow enforces none,
// and the server's own ceiling isn't documented — it just keeps a
// misconfigured request from asking for an absurd amount in one call.
const maxResyncCount = 500

// ResyncRequest is the body of POST /api/resync: a request for WhatsApp to
// push more history for one chat, ending just before a message this bridge
// already has (see BuildHistorySyncRequest). This is on-demand pagination,
// walking further into the past from a known point — it cannot repopulate a
// chat this bridge has no record of at all, since there is no earlier
// message to request history "before".
type ResyncRequest struct {
	ChatJID              string `json:"chat_jid"`
	OldestMessageID      string `json:"oldest_message_id"`
	OldestMessageFromMe  bool   `json:"oldest_message_from_me"`
	OldestMessageUnixSec int64  `json:"oldest_message_timestamp"`
	Count                int    `json:"count"`
}

// ResyncResponse is the response body of POST /api/resync.
type ResyncResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// parseResyncRequest validates a ResyncRequest and builds the
// *types.MessageInfo and message count that Client.BuildHistorySyncRequest
// needs, applying the default and bound on Count.
func parseResyncRequest(req ResyncRequest) (*types.MessageInfo, int, error) {
	if req.ChatJID == "" {
		return nil, 0, fmt.Errorf("chat_jid is required")
	}
	// types.ParseJID barely validates: a bare string with no "@" parses
	// "successfully" into a JID with an empty User, so malformed input must
	// be caught on User rather than on err alone.
	chat, err := types.ParseJID(req.ChatJID)
	if err != nil || chat.User == "" {
		return nil, 0, fmt.Errorf("invalid chat_jid: %q", req.ChatJID)
	}
	if req.OldestMessageID == "" {
		return nil, 0, fmt.Errorf("oldest_message_id is required")
	}
	if req.OldestMessageUnixSec <= 0 {
		return nil, 0, fmt.Errorf("oldest_message_timestamp is required")
	}

	count := req.Count
	if count == 0 {
		count = defaultResyncCount
	}
	if count < 1 || count > maxResyncCount {
		return nil, 0, fmt.Errorf("count must be between 1 and %d", maxResyncCount)
	}

	return &types.MessageInfo{
		MessageSource: types.MessageSource{Chat: chat, IsFromMe: req.OldestMessageFromMe},
		ID:            req.OldestMessageID,
		Timestamp:     time.Unix(req.OldestMessageUnixSec, 0),
	}, count, nil
}
