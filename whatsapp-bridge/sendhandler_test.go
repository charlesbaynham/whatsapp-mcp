package main

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func alwaysReady(http.ResponseWriter) bool { return true }

// serveSend runs one request against the handler, with a send function the test
// supplies. The queue's gate is disabled so only the queueing is exercised.
func serveSend(t *testing.T, store string, r *http.Request, send func(context.Context, SendMessageRequest) (bool, string)) (*httptest.ResponseRecorder, *sendQueue) {
	t.Helper()
	q := newSendQueue(newTestGate(0), send)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)

	w := httptest.NewRecorder()
	sendHandler(q, store, alwaysReady, false)(w, r)
	return w, q
}

func multipartSend(t *testing.T, fields map[string]string, content string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for k, v := range fields {
		mw.WriteField(k, v)
	}
	fw, err := mw.CreateFormFile("file", "photo.jpg")
	if err != nil {
		t.Fatal(err)
	}
	fw.Write([]byte(content))
	mw.Close()

	r := httptest.NewRequest("POST", "/api/send", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	return r
}

// The handler returns long before an async send happens. If it takes the
// upload with it, the worker finds nothing to send.
func TestAsyncUploadSurvivesTheHandlerReturning(t *testing.T) {
	store := t.TempDir()
	type seen struct {
		path    string
		content string
		err     error
	}
	observed := make(chan seen, 1)

	r := multipartSend(t, map[string]string{"recipient": "447700900000"}, "JPEGDATA")
	w, _ := serveSend(t, store, r, func(_ context.Context, req SendMessageRequest) (bool, string) {
		data, err := os.ReadFile(req.MediaPath)
		observed <- seen{path: req.MediaPath, content: string(data), err: err}
		return true, "sent"
	})

	if w.Code != http.StatusAccepted {
		t.Fatalf("status %d, want 202", w.Code)
	}

	select {
	case got := <-observed:
		if got.err != nil {
			t.Fatalf("the worker could not read the upload at %s: %v", got.path, got.err)
		}
		if got.content != "JPEGDATA" {
			t.Fatalf("upload content = %q at send time", got.content)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the send never ran")
	}
}

func TestUploadIsRemovedOnceSent(t *testing.T) {
	store := t.TempDir()
	paths := make(chan string, 1)
	r := multipartSend(t, map[string]string{"recipient": "447700900000"}, "JPEGDATA")
	serveSend(t, store, r, func(_ context.Context, req SendMessageRequest) (bool, string) {
		paths <- req.MediaPath
		return true, "sent"
	})

	path := <-paths
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("upload %s still on disk after the send; the store fills up", path)
}

func TestRejectedUploadIsRemovedImmediately(t *testing.T) {
	store := t.TempDir()
	// No recipient: the handler bails out before the queue takes ownership.
	r := multipartSend(t, map[string]string{"message": "hi"}, "JPEGDATA")
	w, _ := serveSend(t, store, r, func(context.Context, SendMessageRequest) (bool, string) {
		t.Error("a request with no recipient reached the queue")
		return false, ""
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", w.Code)
	}
	entries, err := os.ReadDir(store + "/tmp")
	if err != nil {
		return // nothing was written at all
	}
	if len(entries) != 0 {
		t.Fatalf("%d upload(s) left behind by a rejected request", len(entries))
	}
}

func TestAsyncResponseCarriesAnIDAndQueuePosition(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	store := t.TempDir()
	send := func(context.Context, SendMessageRequest) (bool, string) {
		<-release
		return true, "sent"
	}

	q := newSendQueue(newTestGate(0), send)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)

	var ids []string
	for i := 0; i < 3; i++ {
		r := httptest.NewRequest("POST", "/api/send", strings.NewReader(`{"recipient":"447700900000","message":"hi"}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		sendHandler(q, store, alwaysReady, false)(w, r)

		if w.Code != http.StatusAccepted {
			t.Fatalf("submission %d: status %d, want 202", i, w.Code)
		}
		var res SendMessageResponse
		if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
			t.Fatal(err)
		}
		if !res.Success || !res.Queued || res.ID == "" {
			t.Fatalf("submission %d: %+v", i, res)
		}
		// Never a negative position, whatever the worker is doing meanwhile.
		if strings.Contains(res.Message, "-1 ahead") {
			t.Fatalf("submission %d reported a negative queue position: %q", i, res.Message)
		}
		ids = append(ids, res.ID)
	}

	if ids[0] == ids[1] || ids[1] == ids[2] {
		t.Fatalf("ids repeat: %v", ids)
	}
}

func TestBlockingRequestWaitsForTheOutcome(t *testing.T) {
	store := t.TempDir()
	r := httptest.NewRequest("POST", "/api/send", strings.NewReader(`{"recipient":"447700900000","message":"hi","block":true}`))
	r.Header.Set("Content-Type", "application/json")
	w, _ := serveSend(t, store, r, func(context.Context, SendMessageRequest) (bool, string) {
		return false, "no LID found"
	})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 for a failed send", w.Code)
	}
	var res SendMessageResponse
	json.Unmarshal(w.Body.Bytes(), &res)
	if res.Success || res.Queued || res.Message != "no LID found" {
		t.Fatalf("blocking response = %+v, want the real failure", res)
	}
}

func TestFullQueueRefusesAndRemovesTheUpload(t *testing.T) {
	t.Setenv("WHATSAPP_SEND_MAX_QUEUE_DEPTH", "1")
	store := t.TempDir()
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) {
		<-block
		return true, "sent"
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)

	accepted, refused := 0, 0
	for i := 0; i < 6; i++ {
		w := httptest.NewRecorder()
		sendHandler(q, store, alwaysReady, false)(w, multipartSend(t, map[string]string{"recipient": "447700900000"}, "JPEGDATA"))
		if w.Code == http.StatusServiceUnavailable {
			refused++
			break
		}
		accepted++
		time.Sleep(5 * time.Millisecond)
	}
	if refused == 0 {
		t.Fatal("queue never refused a submission past its depth")
	}

	// Every accepted upload is legitimately still on disk — one in flight, the
	// rest waiting. A refused one is the handler's to remove, since the queue
	// never took it.
	entries, err := os.ReadDir(store + "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != accepted {
		t.Fatalf("%d uploads on disk for %d accepted sends; a refused request left its own behind", len(entries), accepted)
	}
}

func TestFirstContactSendIsCalledOutInTheResponse(t *testing.T) {
	q := newSendQueue(newTestGate(0), func(context.Context, SendMessageRequest) (bool, string) { return true, "sent" })
	q.newContacts = newNewContactStage(newTestGate(30*time.Minute), func(string) bool { return true },
		func() (bool, time.Time) { return false, time.Time{} })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go q.run(ctx)

	var last SendMessageResponse
	for _, recipient := range []string{"447700900000", "447700900001"} {
		body := strings.NewReader(`{"recipient":"` + recipient + `","message":"hi"}`)
		r := httptest.NewRequest("POST", "/api/send", body)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		sendHandler(q, t.TempDir(), alwaysReady, false)(w, r)
		if w.Code != http.StatusAccepted {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		if err := json.Unmarshal(w.Body.Bytes(), &last); err != nil {
			t.Fatal(err)
		}
	}
	// The first went straight out and claimed the gate's next slot; the
	// second is told to expect that slot, however the draw fell.
	if !last.NewContact || last.EstimatedWaitSeconds < 1 || last.EstimatedWaitSeconds > gapMeanCeiling*30*60 {
		t.Fatalf("response = %+v, want a new contact held until the gate's next slot", last)
	}
	if !strings.Contains(last.Message, "First contact") {
		t.Fatalf("message %q does not warn about the hold", last.Message)
	}
}
