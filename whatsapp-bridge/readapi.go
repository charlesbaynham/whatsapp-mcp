package main

// REST handlers for the read side of the bridge. All GET, all JSON, all
// backed by readstore.go. Errors are {"error": "..."} with a 4xx/5xx status.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, map[string]string{"error": fmt.Sprintf(format, args...)})
}

// queryInt parses an integer query parameter, returning def when absent.
func queryInt(r *http.Request, key string, def int) (int, error) {
	s := r.URL.Query().Get(key)
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	return n, nil
}

// queryBool parses a boolean query parameter, returning def when absent.
func queryBool(r *http.Request, key string, def bool) (bool, error) {
	s := r.URL.Query().Get(key)
	if s == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", key)
	}
	return b, nil
}

// queryTime parses an RFC3339 (or date-only) query parameter.
func queryTime(r *http.Request, key string) (*time.Time, error) {
	s := r.URL.Query().Get(key)
	if s == "" {
		return nil, nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t, nil
		}
	}
	return nil, fmt.Errorf("%s must be an ISO-8601 timestamp", key)
}

func getOnly(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return false
	}
	return true
}

func registerReadRoutes(mux *http.ServeMux, store *MessageStore) {
	mux.HandleFunc("/api/chats", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		var p ListChatsParams
		var err error
		p.Query = r.URL.Query().Get("query")
		p.SortBy = r.URL.Query().Get("sort_by")
		if p.Limit, err = queryInt(r, "limit", readDefaultLimit); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.Page, err = queryInt(r, "page", 0); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.IncludeLastMessage, err = queryBool(r, "include_last_message", true); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.UnreadOnly, err = queryBool(r, "unread_only", false); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		chats, err := store.ListChats(p)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list chats: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, chats)
	})

	mux.HandleFunc("/api/chats/unread", func(w http.ResponseWriter, r *http.Request) {
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
		chats, err := store.ListChats(ListChatsParams{Limit: limit, Page: page, IncludeLastMessage: true, UnreadOnly: true})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list unread chats: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, chats)
	})

	mux.HandleFunc("/api/chats/by-phone/{number}", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		chat, found, err := store.GetDirectChatByPhone(r.PathValue("number"))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to look up chat: %v", err)
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "no direct chat for %q", r.PathValue("number"))
			return
		}
		writeJSON(w, http.StatusOK, chat)
	})

	mux.HandleFunc("/api/chats/{jid}", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		include, err := queryBool(r, "include_last_message", true)
		if err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		chat, found, err := store.GetChat(r.PathValue("jid"), include)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to look up chat: %v", err)
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "chat %q not found", r.PathValue("jid"))
			return
		}
		writeJSON(w, http.StatusOK, chat)
	})

	mux.HandleFunc("/api/contacts", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		contacts, err := store.SearchContacts(r.URL.Query().Get("query"))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to search contacts: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, contacts)
	})

	mux.HandleFunc("/api/contacts/{jid}/chats", func(w http.ResponseWriter, r *http.Request) {
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
		chats, err := store.GetContactChats(r.PathValue("jid"), limit, page)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list contact chats: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, chats)
	})

	mux.HandleFunc("/api/contacts/{jid}/last-interaction", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		msg, found, err := store.GetLastInteraction(r.PathValue("jid"))
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to look up last interaction: %v", err)
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "no messages involving %q", r.PathValue("jid"))
			return
		}
		writeJSON(w, http.StatusOK, msg)
	})

	mux.HandleFunc("/api/messages", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		var p ListMessagesParams
		var err error
		q := r.URL.Query()
		p.Sender = q.Get("sender")
		p.ChatJID = q.Get("chat_jid")
		p.Query = q.Get("query")
		if p.After, err = queryTime(r, "after"); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.Before, err = queryTime(r, "before"); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.Limit, err = queryInt(r, "limit", readDefaultLimit); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.Page, err = queryInt(r, "page", 0); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.IncludeContext, err = queryBool(r, "include_context", false); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.ContextBefore, err = queryInt(r, "context_before", 1); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if p.ContextAfter, err = queryInt(r, "context_after", 1); err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		msgs, err := store.ListMessages(p)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list messages: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, msgs)
	})

	mux.HandleFunc("/api/messages/{id}/context", func(w http.ResponseWriter, r *http.Request) {
		if !getOnly(w, r) {
			return
		}
		before, err := queryInt(r, "before", 5)
		if err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		after, err := queryInt(r, "after", 5)
		if err != nil {
			writeError(w, http.StatusBadRequest, "%v", err)
			return
		}
		ctx, found, err := store.GetMessageContext(r.PathValue("id"), r.URL.Query().Get("chat_jid"), before, after)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to look up message: %v", err)
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "message %q not found", r.PathValue("id"))
			return
		}
		writeJSON(w, http.StatusOK, ctx)
	})
}
