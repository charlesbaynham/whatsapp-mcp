package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// queuedMessage describes where a submission landed. A first contact is
// called out explicitly, with the wait it implies: a caller expecting the
// usual half-minute would otherwise take a half-hour hold for a failure.
func queuedMessage(id string, pos queuePosition) string {
	if !pos.newContact {
		return fmt.Sprintf("Queued as %s, %d ahead of it", id, pos.ahead)
	}
	return fmt.Sprintf("Queued as %s. First contact: this recipient has never been messaged from this account, "+
		"so it is held in the new-contact queue (%d new contacts ahead of it) and expected to go out in roughly %s",
		id, pos.ahead, roughDuration(pos.wait))
}

// roughDuration renders an estimate at the precision an estimate deserves.
func roughDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "under a minute"
	case d < time.Hour:
		return fmt.Sprintf("%d min", int(d.Round(time.Minute)/time.Minute))
	default:
		return fmt.Sprintf("%.1f h", d.Hours())
	}
}

// sendHandler accepts a message and hands it to the queue. ready reports
// whether WhatsApp is connected, injected so the handler is testable without a
// live client.
func sendHandler(queue *sendQueue, storeDir string, ready func(http.ResponseWriter) bool, logBodies bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !ready(w) {
			return
		}

		req, cleanup, err := parseSendRequest(r, storeDir)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// `defer cleanup()` would bind the closure parseSendRequest returned,
		// and go on deleting the upload even after the queue takes ownership of
		// it below — for an async send, before the worker has read the file.
		defer func() { cleanup() }()

		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}
		if req.Message == "" && req.MediaPath == "" && req.Poll == nil {
			http.Error(w, "Message, media path or poll is required", http.StatusBadRequest)
			return
		}
		if req.Poll != nil {
			if req.MediaPath != "" {
				http.Error(w, "A poll cannot carry media", http.StatusBadRequest)
				return
			}
			spec, err := req.Poll.validate()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			req.poll = &spec
		}

		switch {
		case req.poll != nil && logBodies:
			fmt.Println("Received request to send poll", req.poll.Question, req.poll.Options)
		case req.poll != nil:
			fmt.Println("Received request to send poll to", req.Recipient)
		case logBodies:
			fmt.Println("Received request to send message", req.Message, req.MediaPath)
		default:
			fmt.Println("Received request to send message to", req.Recipient)
		}

		job, pos, queued := queue.submit(req, cleanup)
		if !queued {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(SendMessageResponse{
				Success: false,
				Message: "Send queue is full; the rate limit is still working through the backlog",
			})
			return
		}
		cleanup = func() {} // the job owns the upload now and cleans up after sending

		w.Header().Set("Content-Type", "application/json")
		if !req.Block {
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(SendMessageResponse{
				Success:              true,
				Queued:               true,
				NewContact:           pos.newContact,
				Ahead:                pos.ahead,
				EstimatedWaitSeconds: int(pos.wait / time.Second),
				ID:                   job.id,
				Message:              queuedMessage(job.id, pos),
			})
			return
		}

		select {
		case res := <-job.done:
			if !res.Success {
				w.WriteHeader(http.StatusInternalServerError)
			}
			json.NewEncoder(w).Encode(SendMessageResponse{
				Success: res.Success,
				Message: res.Message,
				ID:      res.ID,
			})
		case <-r.Context().Done():
			// The caller gave up; the job stays queued and still goes out.
			fmt.Printf("Caller stopped waiting for %s\n", job.id)
		}
	}
}
