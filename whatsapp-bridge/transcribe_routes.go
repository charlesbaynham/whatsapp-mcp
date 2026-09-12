package main

import "net/http"

// registerTranscribeRoutes adds the on-demand backfill endpoint. It is the
// one case where a transcript arrives as a second (message.updated) event,
// and it is caller-initiated.
func registerTranscribeRoutes(mux *http.ServeMux, store *MessageStore, transcriber *Transcriber) {
	mux.HandleFunc("/api/messages/{chat_jid}/{id}/transcribe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if !transcriber.Enabled() {
			writeError(w, http.StatusServiceUnavailable, "transcription is disabled (WHATSAPP_TRANSCRIBE is not 1)")
			return
		}
		chatJID, id := r.PathValue("chat_jid"), r.PathValue("id")
		ev, found, err := store.messageEvent(chatJID, id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to look up message: %v", err)
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "message %q not found in %q", id, chatJID)
			return
		}
		if ev.MediaType != "audio" {
			writeError(w, http.StatusBadRequest, "message %q is not a voice note", id)
			return
		}
		if err := store.markTranscriptionPending(chatJID, id); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to queue: %v", err)
			return
		}
		transcriber.Enqueue(transcribeJob{chatJID: chatJID, messageID: id, update: true})
		writeJSON(w, http.StatusAccepted, map[string]any{
			"success": true,
			"message": "queued; a message.updated event follows when the transcript is ready",
		})
	})
}
