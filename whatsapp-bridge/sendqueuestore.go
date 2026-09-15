package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// parseSQLiteTime parses a timestamp the way the sqlite3 driver would have,
// had it known the column's declared type — which it does for an ordinary
// column read, but not for the result of an aggregate like MAX(due_at), so
// that comes back as a plain string instead of a time.Time.
func parseSQLiteTime(s string) (time.Time, error) {
	var lastErr error
	for _, format := range sqlite3.SQLiteTimestampFormats {
		if t, err := time.Parse(format, s); err == nil {
			return t, nil
		} else {
			lastErr = err
		}
	}
	return time.Time{}, lastErr
}

// sendQueueStore persists the send queue's submissions and scheduling state
// to a small sqlite database in the store dir — sendqueue.db, deliberately
// separate from messages.db so this never touches MessageStore's schema.
// It exists so the bridge's "cattle" container, destroyed and recreated on
// every deploy, does not abandon whatever was still waiting behind the rate
// limit: every row is written before its submission is acknowledged, updated
// once its send finishes, and reloaded on the next start.
type sendQueueStore struct {
	db       *sql.DB
	storeDir string
}

// finishedResultsKept mirrors resultsKept: no point keeping more finished
// rows on disk than /api/send/{id} ever keeps in memory.
const finishedResultsKept = resultsKept

// finishedRetention bounds how long a finished row is kept on disk even if
// the bridge never accumulates finishedResultsKept of them.
const finishedRetention = 7 * 24 * time.Hour

// openSendQueueStore opens (creating if needed) the durable queue database
// under storeDir. Callers should treat any error as reason to fall back to a
// memory-only queue rather than refuse to serve sends at all.
func openSendQueueStore(storeDir string) (*sendQueueStore, error) {
	if err := os.MkdirAll(storeDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating store directory: %v", err)
	}
	dbPath := filepath.Join(storeDir, "sendqueue.db")
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s", dbPath))
	if err != nil {
		return nil, fmt.Errorf("opening %s: %v", dbPath, err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("opening %s: %v", dbPath, err)
	}
	if _, err := db.Exec(sendQueueSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating sendqueue schema: %v", err)
	}
	return &sendQueueStore{db: db, storeDir: storeDir}, nil
}

const sendQueueSchema = `
CREATE TABLE IF NOT EXISTS sendqueue_jobs (
	id TEXT PRIMARY KEY,
	recipient TEXT NOT NULL,
	message TEXT NOT NULL DEFAULT '',
	media_path TEXT NOT NULL DEFAULT '',
	upload_name TEXT NOT NULL DEFAULT '',
	voice_note INTEGER NOT NULL DEFAULT 0,
	poll TEXT NOT NULL DEFAULT '',
	new_contact INTEGER NOT NULL DEFAULT 0,
	-- Set once, at submission, for a new-contact row and never touched again:
	-- due_at below moves when the row is relocated to the main queue on
	-- release, but a stage's own lastDue must still be recoverable from the
	-- due time it originally handed out, so that is kept here separately.
	new_contact_due_at TIMESTAMP,
	stage TEXT NOT NULL DEFAULT 'main',
	due_at TIMESTAMP NOT NULL,
	queued_at TIMESTAMP NOT NULL,
	state TEXT NOT NULL DEFAULT 'queued',
	success INTEGER NOT NULL DEFAULT 0,
	outcome TEXT NOT NULL DEFAULT '',
	sent_at TIMESTAMP
);
CREATE INDEX IF NOT EXISTS sendqueue_jobs_state_stage ON sendqueue_jobs (state, stage, due_at);
`

func (s *sendQueueStore) Close() error {
	return s.db.Close()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// insertJob writes a brand-new submission's row. Called once, before the
// submission is acknowledged.
func (s *sendQueueStore) insertJob(job *sendJob, newContact bool, stage string, dueAt, queuedAt time.Time) error {
	pollJSON := ""
	if job.req.poll != nil {
		b, err := json.Marshal(job.req.poll)
		if err != nil {
			return fmt.Errorf("encoding poll: %v", err)
		}
		pollJSON = string(b)
	}
	var newContactDueAt any
	if newContact {
		newContactDueAt = dueAt
	}
	_, err := s.db.Exec(`
		INSERT INTO sendqueue_jobs
			(id, recipient, message, media_path, upload_name, voice_note, poll,
			 new_contact, new_contact_due_at, stage, due_at, queued_at, state, success, outcome, sent_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', 0, '', NULL)`,
		job.id, job.req.Recipient, job.req.Message, job.req.MediaPath, job.req.uploadName,
		boolInt(job.req.VoiceNote), pollJSON, boolInt(newContact), newContactDueAt, stage, dueAt, queuedAt,
	)
	return err
}

// finishJob records a submission's outcome. The row's stage/due_at are left
// alone: they describe where it was scheduled, not how it went.
func (s *sendQueueStore) finishJob(id string, success bool, message string, sentAt time.Time) error {
	state := sendFailed
	if success {
		state = sendSent
	}
	_, err := s.db.Exec(`UPDATE sendqueue_jobs SET state = ?, success = ?, outcome = ?, sent_at = ? WHERE id = ?`,
		string(state), boolInt(success), message, sentAt, id)
	return err
}

// setSchedule updates a row's current stage and due time — used when a job
// is relocated from the new-contact stage to the main queue, and when a
// delay shifts everything held behind it.
func (s *sendQueueStore) setSchedule(id, stage string, dueAt time.Time) error {
	_, err := s.db.Exec(`UPDATE sendqueue_jobs SET stage = ?, due_at = ? WHERE id = ?`, stage, dueAt, id)
	return err
}

// persistedRow is one row loaded back from disk.
type persistedRow struct {
	ID         string
	Recipient  string
	Message    string
	MediaPath  string
	UploadName string
	VoiceNote  bool
	Poll       *PollSpec
	NewContact bool
	DueAt      time.Time
	QueuedAt   time.Time
}

// request rebuilds the SendMessageRequest a recovered job needs to replay.
func (r persistedRow) request() SendMessageRequest {
	return SendMessageRequest{
		Recipient: r.Recipient, Message: r.Message, MediaPath: r.MediaPath,
		VoiceNote: r.VoiceNote, uploadName: r.UploadName, poll: r.Poll,
	}
}

// loadQueued returns every row still queued in the given stage, in the order
// they are owed to leave (their due time; ties broken by submission order).
func (s *sendQueueStore) loadQueued(stage string) ([]persistedRow, error) {
	rows, err := s.db.Query(`
		SELECT id, recipient, message, media_path, upload_name, voice_note, poll, new_contact, due_at, queued_at
		FROM sendqueue_jobs WHERE state = ? AND stage = ? ORDER BY due_at ASC, rowid ASC`,
		string(sendQueued), stage)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []persistedRow
	for rows.Next() {
		var r persistedRow
		var pollJSON string
		var voiceNote, newContact int
		if err := rows.Scan(&r.ID, &r.Recipient, &r.Message, &r.MediaPath, &r.UploadName,
			&voiceNote, &pollJSON, &newContact, &r.DueAt, &r.QueuedAt); err != nil {
			return nil, err
		}
		r.VoiceNote = voiceNote != 0
		r.NewContact = newContact != 0
		if pollJSON != "" {
			var spec PollSpec
			if err := json.Unmarshal([]byte(pollJSON), &spec); err != nil {
				return nil, fmt.Errorf("row %s has a corrupt poll: %v", r.ID, err)
			}
			r.Poll = &spec
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// getResult reads back one row as a SendResult, whatever its state — used
// when a submission finished (or was never seen at all) in an earlier
// process, so it is not in memory anymore for this one to answer from.
func (s *sendQueueStore) getResult(id string) (SendResult, bool, error) {
	var (
		state      string
		success    int
		message    string
		newContact int
		queuedAt   time.Time
		sentAt     sql.NullTime
	)
	err := s.db.QueryRow(`
		SELECT state, success, outcome, new_contact, queued_at, sent_at
		FROM sendqueue_jobs WHERE id = ?`, id,
	).Scan(&state, &success, &message, &newContact, &queuedAt, &sentAt)
	if err == sql.ErrNoRows {
		return SendResult{}, false, nil
	}
	if err != nil {
		return SendResult{}, false, err
	}
	res := SendResult{
		ID: id, State: sendState(state), Success: success != 0, Message: message,
		NewContact: newContact != 0, QueuedAt: queuedAt,
	}
	if sentAt.Valid {
		res.SentAt = sentAt.Time
	}
	return res, true, nil
}

// maxDueAt is the newest due time ever assigned to a row currently (or
// still) in the given stage, queued or finished — used to seed lastDue.
func (s *sendQueueStore) maxDueAt(stage string) (time.Time, error) {
	var v sql.NullString
	if err := s.db.QueryRow(`SELECT MAX(due_at) FROM sendqueue_jobs WHERE stage = ?`, stage).Scan(&v); err != nil {
		return time.Time{}, err
	}
	if !v.Valid || v.String == "" {
		return time.Time{}, nil
	}
	return parseSQLiteTime(v.String)
}

// maxNewContactDueAt is the newest due time ever handed out by the
// new-contact stage, whether or not the row has since been released to the
// main queue (which would otherwise have overwritten due_at/stage). See the
// new_contact_due_at column comment in the schema.
func (s *sendQueueStore) maxNewContactDueAt() (time.Time, error) {
	var v sql.NullString
	if err := s.db.QueryRow(`SELECT MAX(new_contact_due_at) FROM sendqueue_jobs WHERE new_contact_due_at IS NOT NULL`).Scan(&v); err != nil {
		return time.Time{}, err
	}
	if !v.Valid || v.String == "" {
		return time.Time{}, nil
	}
	return parseSQLiteTime(v.String)
}

// prune drops finished rows beyond what /api/send/{id} could ever still
// serve: older than finishedRetention, or past the newest
// finishedResultsKept of them. Queued rows are never touched.
func (s *sendQueueStore) prune() error {
	cutoff := time.Now().Add(-finishedRetention)
	_, err := s.db.Exec(`
		DELETE FROM sendqueue_jobs
		WHERE state != 'queued'
		  AND (queued_at < ?
		       OR id NOT IN (
		         SELECT id FROM sendqueue_jobs WHERE state != 'queued' ORDER BY rowid DESC LIMIT ?
		       ))`,
		cutoff, finishedResultsKept)
	return err
}

// sweepOrphanUploads removes files under <storeDir>/tmp/upload-* that no
// still-queued row references: an upload whose send already finished (its
// own cleanup already removed it — this is a backstop, not the common case)
// or one from a submission that crashed between writing the file and
// persisting its row. referenced holds the media paths recovery just loaded;
// nothing outside tmp/upload-* is ever touched.
func sweepOrphanUploads(storeDir string, referenced map[string]bool) (removed int, err error) {
	matches, globErr := filepath.Glob(filepath.Join(storeDir, "tmp", "upload-*"))
	if globErr != nil {
		return 0, globErr
	}
	for _, path := range matches {
		if referenced[path] {
			continue
		}
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			err = rmErr
			continue
		}
		removed++
	}
	return removed, err
}

// uploadCleanup rebuilds a recovered job's cleanup: only a multipart upload
// (uploadName set) owns its file and must remove it once sent; a media_path
// given directly in a JSON request is the caller's own file and is never
// touched.
func uploadCleanup(r persistedRow) func() {
	if r.MediaPath == "" || r.UploadName == "" {
		return func() {}
	}
	path := r.MediaPath
	return func() { os.Remove(path) }
}
