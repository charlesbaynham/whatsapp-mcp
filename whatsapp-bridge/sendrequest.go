package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// maxUploadBytes caps a multipart upload to /api/send.
const maxUploadBytes = 100 << 20

// parseSendRequest decodes POST /api/send from either a JSON body
// (recipient, message, media_path, voice_note) or multipart/form-data with
// the same fields plus a `file` part. An uploaded file is written under the
// store's tmp dir so the usual containment check passes, and cleanup removes
// it once the send completes.
func parseSendRequest(r *http.Request, storeDir string) (SendMessageRequest, func(), error) {
	noop := func() {}
	ct := r.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "multipart/form-data") {
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return req, noop, fmt.Errorf("Invalid request format")
		}
		return req, noop, nil
	}

	r.Body = http.MaxBytesReader(nil, r.Body, maxUploadBytes)
	if err := r.ParseMultipartForm(8 << 20); err != nil {
		return SendMessageRequest{}, noop, fmt.Errorf("invalid multipart body: %v", err)
	}
	req := SendMessageRequest{
		Recipient: r.FormValue("recipient"),
		Message:   r.FormValue("message"),
		MediaPath: r.FormValue("media_path"),
	}
	if v := r.FormValue("voice_note"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return req, noop, fmt.Errorf("voice_note must be true or false")
		}
		req.VoiceNote = b
	}

	file, header, err := r.FormFile("file")
	if err == http.ErrMissingFile {
		return req, noop, nil
	}
	if err != nil {
		return req, noop, fmt.Errorf("reading file part: %v", err)
	}
	defer file.Close()

	name, err := sanitizeMediaFilename(header.Filename)
	if err != nil {
		name = "upload"
	}
	tmpDir := filepath.Join(storeDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return req, noop, fmt.Errorf("creating tmp dir: %v", err)
	}
	// Keep the original extension: sendWhatsAppMessage picks the media type from it.
	tmp, err := os.CreateTemp(tmpDir, "upload-*"+filepath.Ext(name))
	if err != nil {
		return req, noop, fmt.Errorf("creating temp file: %v", err)
	}
	if _, err := io.Copy(tmp, file); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return req, noop, fmt.Errorf("saving upload: %v", err)
	}
	tmp.Close()
	req.MediaPath = tmp.Name()
	// A document is titled by its filename; the temp name would leak otherwise.
	req.uploadName = name
	return req, func() { os.Remove(tmp.Name()) }, nil
}
