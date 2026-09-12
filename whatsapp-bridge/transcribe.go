package main

// Local transcription of incoming voice notes with whisper.cpp.
//
// Publication of a voice note is gated on this: the message is stored on
// arrival but its message.new event (and so every webhook and SSE consumer)
// waits until the transcript is in, so consumers see exactly one event per
// voice note with the text attached. One serialised worker, a per-job
// timeout, and a restart sweep of still-pending rows keep the gate from
// ever holding a message forever.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	transcriptionPending  = "pending"
	transcriptionOK       = "ok"
	transcriptionFailed   = "failed"
	transcriptionTimeout  = "timeout"
	transcriptionDisabled = "disabled"

	transcribeQueueSize      = 256
	defaultTranscribeTimeout = 120 * time.Second
	whisperModelBaseURL      = "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/"
)

// transcribeJob names one voice note to transcribe. update=true republishes
// as message.updated (an on-demand backfill) instead of message.new.
type transcribeJob struct {
	chatJID   string
	messageID string
	update    bool
}

// TranscriberConfig is read from the environment in newTranscriberFromEnv.
type TranscriberConfig struct {
	Enabled   bool
	ModelPath string
	ModelURL  string
	Binary    string
	Timeout   time.Duration
}

func newTranscriberConfigFromEnv(storeDir string) TranscriberConfig {
	cfg := TranscriberConfig{
		Enabled:   os.Getenv("WHATSAPP_TRANSCRIBE") == "1",
		ModelPath: getEnvOrDefault("WHATSAPP_WHISPER_MODEL", filepath.Join(storeDir, "models", "ggml-base.bin")),
		Binary:    getEnvOrDefault("WHATSAPP_WHISPER_BIN", "whisper-cli"),
		Timeout:   defaultTranscribeTimeout,
	}
	cfg.ModelURL = getEnvOrDefault("WHATSAPP_WHISPER_MODEL_URL", whisperModelBaseURL+filepath.Base(cfg.ModelPath))
	if s := os.Getenv("WHATSAPP_TRANSCRIBE_TIMEOUT"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			cfg.Timeout = time.Duration(n) * time.Second
		}
	}
	return cfg
}

// Transcriber owns the queue and the single worker goroutine.
type Transcriber struct {
	cfg    TranscriberConfig
	store  *MessageStore
	pub    *Publisher
	logger waLog.Logger

	// fetch returns a local path to the voice note's audio. Injected so tests
	// need no whatsmeow client.
	fetch func(ctx context.Context, chatJID, messageID string) (string, error)
	// run transcribes an audio file to text. Injected so tests need no
	// whisper binary or model.
	run func(ctx context.Context, audioPath string) (string, error)

	queue    chan transcribeJob
	once     sync.Once
	modelMu  sync.Mutex
	modelErr error
}

func NewTranscriber(cfg TranscriberConfig, store *MessageStore, pub *Publisher, logger waLog.Logger) *Transcriber {
	t := &Transcriber{cfg: cfg, store: store, pub: pub, logger: logger, queue: make(chan transcribeJob, transcribeQueueSize)}
	t.run = t.runWhisper
	return t
}

// Enabled reports whether voice notes are gated on transcription at all.
func (t *Transcriber) Enabled() bool {
	return t != nil && t.cfg.Enabled
}

// Start launches the worker and re-enqueues anything left pending by a
// previous run, so a restart mid-transcription loses nothing.
func (t *Transcriber) Start() {
	t.once.Do(func() {
		go t.worker()
		pending, err := t.store.pendingTranscriptions()
		if err != nil {
			t.logger.Warnf("transcribe: failed to list pending rows: %v", err)
			return
		}
		for _, j := range pending {
			t.Enqueue(j)
		}
		if len(pending) > 0 {
			t.logger.Infof("transcribe: re-queued %d pending voice note(s) from before restart", len(pending))
		}
	})
}

// Enqueue adds a job. A full queue fails the message open (published with
// status failed) rather than blocking the receive path.
func (t *Transcriber) Enqueue(job transcribeJob) {
	select {
	case t.queue <- job:
	default:
		t.logger.Warnf("transcribe: queue full, publishing %s without transcript", job.messageID)
		t.finish(job, "", transcriptionFailed)
	}
}

func (t *Transcriber) worker() {
	for job := range t.queue {
		t.process(job)
	}
}

func (t *Transcriber) process(job transcribeJob) {
	ctx, cancel := context.WithTimeout(context.Background(), t.cfg.Timeout)
	defer cancel()

	audioPath, err := t.fetch(ctx, job.chatJID, job.messageID)
	if err != nil {
		t.logger.Warnf("transcribe %s: fetch failed: %v", job.messageID, err)
		t.finish(job, "", transcriptionFailed)
		return
	}
	text, err := t.run(ctx, audioPath)
	switch {
	case err == nil:
		t.finish(job, cleanTranscript(text), transcriptionOK)
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		t.logger.Warnf("transcribe %s: timed out after %s", job.messageID, t.cfg.Timeout)
		t.finish(job, "", transcriptionTimeout)
	default:
		t.logger.Warnf("transcribe %s: %v", job.messageID, err)
		t.finish(job, "", transcriptionFailed)
	}
}

// finish records the outcome and releases the message to consumers.
func (t *Transcriber) finish(job transcribeJob, text, status string) {
	if err := t.store.setTranscription(job.chatJID, job.messageID, text, status); err != nil {
		t.logger.Warnf("transcribe %s: failed to store result: %v", job.messageID, err)
	}
	if t.pub == nil {
		return
	}
	ev, found, err := t.store.messageEvent(job.chatJID, job.messageID)
	if err != nil || !found {
		t.logger.Warnf("transcribe %s: cannot rebuild event for publication (found=%v err=%v)", job.messageID, found, err)
		return
	}
	if job.update {
		t.pub.PublishMessageUpdated(ev)
	} else {
		t.pub.PublishMessage(ev)
	}
}

// runWhisper decodes the note to 16 kHz mono WAV with ffmpeg and runs
// whisper-cli on it, returning the plain text.
func (t *Transcriber) runWhisper(ctx context.Context, audioPath string) (string, error) {
	if err := t.ensureModel(ctx); err != nil {
		return "", err
	}
	wav, err := os.CreateTemp(filepath.Join(t.store.StoreDir, "tmp"), "whisper-*.wav")
	if err != nil {
		return "", fmt.Errorf("creating temp wav: %w", err)
	}
	wavPath := wav.Name()
	wav.Close()
	defer os.Remove(wavPath)

	ff := exec.CommandContext(ctx, "ffmpeg", "-i", audioPath, "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le", "-y", wavPath)
	if out, err := ff.CombinedOutput(); err != nil {
		return "", fmt.Errorf("ffmpeg decode failed: %v: %s", err, tailOf(out, 400))
	}

	bin := t.cfg.Binary
	if _, err := exec.LookPath(bin); err != nil && bin == "whisper-cli" {
		// Older whisper.cpp packages install the CLI under the package name.
		if _, err2 := exec.LookPath("whisper-cpp"); err2 == nil {
			bin = "whisper-cpp"
		}
	}
	cmd := exec.CommandContext(ctx, bin, "-m", t.cfg.ModelPath, "-f", wavPath, "-l", "auto", "-nt", "-np")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s failed: %v: %s", bin, err, tailOf(stderr.Bytes(), 400))
	}
	return stdout.String(), nil
}

// cleanTranscript collapses whisper's line-per-segment output into one
// paragraph and drops its non-speech markers.
func cleanTranscript(raw string) string {
	var parts []string
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || (strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]")) || (strings.HasPrefix(line, "(") && strings.HasSuffix(line, ")")) {
			continue
		}
		parts = append(parts, line)
	}
	return strings.Join(parts, " ")
}

func tailOf(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		s = s[len(s)-n:]
	}
	return s
}

// ensureModel downloads the whisper model on first use if it is missing.
// The result is remembered for the process lifetime so a bad URL fails
// each job fast instead of re-downloading.
func (t *Transcriber) ensureModel(ctx context.Context) error {
	t.modelMu.Lock()
	defer t.modelMu.Unlock()
	if t.modelErr != nil {
		return t.modelErr
	}
	if _, err := os.Stat(t.cfg.ModelPath); err == nil {
		return nil
	}
	t.logger.Infof("transcribe: downloading whisper model %s from %s", filepath.Base(t.cfg.ModelPath), t.cfg.ModelURL)
	if err := downloadFile(ctx, t.cfg.ModelURL, t.cfg.ModelPath); err != nil {
		t.modelErr = fmt.Errorf("whisper model unavailable: %w", err)
		return t.modelErr
	}
	return nil
}

func downloadFile(ctx context.Context, url, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d fetching %s", resp.StatusCode, url)
	}
	part := dest + ".part"
	f, err := os.Create(part)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(part)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(part)
		return err
	}
	return os.Rename(part, dest)
}

// --- store ---

// ensureColumn adds a column if the table lacks it. Idempotent.
func ensureColumn(db *sql.DB, table, column, ddl string) error {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, column, ddl))
	return err
}

func (store *MessageStore) setTranscription(chatJID, messageID, text, status string) error {
	_, err := store.db.Exec(`UPDATE messages SET transcript = ?, transcription_status = ? WHERE chat_jid = ? AND id = ?`,
		text, status, chatJID, messageID)
	return err
}

func (store *MessageStore) markTranscriptionPending(chatJID, messageID string) error {
	return store.setTranscription(chatJID, messageID, "", transcriptionPending)
}

func (store *MessageStore) pendingTranscriptions() ([]transcribeJob, error) {
	rows, err := store.db.Query(`SELECT chat_jid, id FROM messages WHERE transcription_status = ? ORDER BY rowid ASC`, transcriptionPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []transcribeJob
	for rows.Next() {
		var j transcribeJob
		if err := rows.Scan(&j.chatJID, &j.messageID); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// messageEvent rebuilds the publishable payload for a stored message.
func (store *MessageStore) messageEvent(chatJID, messageID string) (WebhookEvent, bool, error) {
	var ev WebhookEvent
	var ts sql.NullTime
	var chatName, content, mediaType, filename, transcript, status sql.NullString
	err := store.db.QueryRow(
		`SELECT m.sender, c.name, m.content, m.timestamp, m.is_from_me, m.media_type, m.filename, m.transcript, m.transcription_status
		 FROM messages m JOIN chats c ON c.jid = m.chat_jid WHERE m.chat_jid = ? AND m.id = ?`,
		chatJID, messageID,
	).Scan(&ev.Sender, &chatName, &content, &ts, &ev.IsFromMe, &mediaType, &filename, &transcript, &status)
	if err == sql.ErrNoRows {
		return WebhookEvent{}, false, nil
	}
	if err != nil {
		return WebhookEvent{}, false, err
	}
	ev.MessageID = messageID
	ev.ChatJID = chatJID
	ev.ChatName = chatName.String
	ev.Content = content.String
	if ts.Valid {
		ev.Timestamp = ts.Time.Format(time.RFC3339)
	}
	ev.MediaType = mediaType.String
	ev.Filename = filename.String
	ev.HasMedia = ev.MediaType != ""
	ev.Transcript = transcript.String
	ev.TranscriptionStatus = status.String
	if ev.MediaType == "audio" {
		ev.DurationSeconds = store.audioDuration(chatJID, messageID)
	}
	return ev, true, nil
}

// audioDuration reads the duration from an already-downloaded voice note,
// or 0 if it isn't on disk.
func (store *MessageStore) audioDuration(chatJID, messageID string) int {
	var filename string
	if err := store.db.QueryRow(`SELECT filename FROM messages WHERE chat_jid = ? AND id = ?`, chatJID, messageID).Scan(&filename); err != nil {
		return 0
	}
	safe, err := sanitizeMediaFilename(filename)
	if err != nil {
		return 0
	}
	path := filepath.Join(store.StoreDir, strings.ReplaceAll(chatJID, ":", "_"), safe)
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	secs, _, err := analyzeOggOpus(data)
	if err != nil {
		return 0
	}
	return int(secs)
}
