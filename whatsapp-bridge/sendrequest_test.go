package main

import (
	"bytes"
	"mime/multipart"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseSendRequestJSON(t *testing.T) {
	r := httptest.NewRequest("POST", "/api/send", strings.NewReader(`{"recipient":"123","message":"hi","voice_note":true}`))
	r.Header.Set("Content-Type", "application/json")
	req, cleanup, err := parseSendRequest(r, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if req.Recipient != "123" || req.Message != "hi" || !req.VoiceNote {
		t.Errorf("parsed %+v", req)
	}
}

func TestParseSendRequestMultipart(t *testing.T) {
	store := t.TempDir()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	mw.WriteField("recipient", "123@s.whatsapp.net")
	mw.WriteField("message", "caption")
	mw.WriteField("voice_note", "false")
	fw, _ := mw.CreateFormFile("file", "../../evil/report.pdf")
	fw.Write([]byte("%PDF-1.4"))
	mw.Close()

	r := httptest.NewRequest("POST", "/api/send", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	req, cleanup, err := parseSendRequest(r, store)
	if err != nil {
		t.Fatal(err)
	}
	if req.Recipient != "123@s.whatsapp.net" || req.Message != "caption" || req.VoiceNote {
		t.Errorf("parsed %+v", req)
	}
	if req.uploadName != "report.pdf" {
		t.Errorf("uploadName = %q, want sanitised base name", req.uploadName)
	}
	if !strings.HasPrefix(req.MediaPath, filepath.Join(store, "tmp")) || !strings.HasSuffix(req.MediaPath, ".pdf") {
		t.Errorf("media path %q not under store tmp with original extension", req.MediaPath)
	}
	if _, err := containExistingPath(store, req.MediaPath); err != nil {
		t.Errorf("upload must pass containment: %v", err)
	}
	data, _ := os.ReadFile(req.MediaPath)
	if string(data) != "%PDF-1.4" {
		t.Errorf("upload content = %q", data)
	}
	cleanup()
	if _, err := os.Stat(req.MediaPath); !os.IsNotExist(err) {
		t.Error("cleanup did not remove the temp upload")
	}
}

func TestDocumentTitle(t *testing.T) {
	if got := documentTitle("", "/x/y/upload-123.pdf"); got != "upload-123.pdf" {
		t.Errorf("got %q", got)
	}
	if got := documentTitle("report.pdf", "/x/y/upload-123.pdf"); got != "report.pdf" {
		t.Errorf("got %q", got)
	}
}
