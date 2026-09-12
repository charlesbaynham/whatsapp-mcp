package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"bytes"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// getEnvOrDefault returns the named environment variable, or def if it is unset or empty.
func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db       *sql.DB
	StoreDir string
}

// Initialize message store
func NewMessageStore(storeDir string) (*MessageStore, error) {
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	dbPath := filepath.Join(storeDir, "messages.db")
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP,
			last_read_timestamp TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	if err := createWebhookSubscriptionsTable(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := createEventsTable(db); err != nil {
		db.Close()
		return nil, err
	}
	for col, ddl := range map[string]string{"transcript": "TEXT", "transcription_status": "TEXT NOT NULL DEFAULT ''"} {
		if err := ensureColumn(db, "messages", col, ddl); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to add messages.%s: %v", col, err)
		}
	}

	// Gated on the column being absent: must only run once, or it would wipe real unread state.
	hasReadTimestampCol := false
	colRows, err := db.Query(`PRAGMA table_info(chats)`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to inspect chats schema: %v", err)
	}
	for colRows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue any
		if err := colRows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			colRows.Close()
			db.Close()
			return nil, fmt.Errorf("failed to read chats schema: %v", err)
		}
		if name == "last_read_timestamp" {
			hasReadTimestampCol = true
		}
	}
	colRows.Close()

	if !hasReadTimestampCol {
		if _, err = db.Exec(`ALTER TABLE chats ADD COLUMN last_read_timestamp TIMESTAMP`); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to add last_read_timestamp column: %v", err)
		}
		if _, err = db.Exec(`UPDATE chats SET last_read_timestamp = last_message_time`); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to backfill last_read_timestamp: %v", err)
		}
	}

	return &MessageStore{db: db, StoreDir: storeDir}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Upsert, not INSERT OR REPLACE: REPLACE would null out last_read_timestamp on every message.
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		`INSERT INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)
		 ON CONFLICT(jid) DO UPDATE SET name = excluded.name, last_message_time = excluded.last_message_time`,
		jid, name, lastMessageTime,
	)
	return err
}

// Forward-only; julianday() because stored RFC3339Nano text isn't safely comparable across offsets.
func (store *MessageStore) MarkChatRead(chatJID string, upTo time.Time) error {
	_, err := store.db.Exec(
		`UPDATE chats SET last_read_timestamp = ?
		 WHERE jid = ? AND (last_read_timestamp IS NULL OR julianday(last_read_timestamp) < julianday(?))`,
		upTo, chatJID, upTo,
	)
	return err
}

// Non-monotonic set; zero Time clears the marker to NULL.
func (store *MessageStore) SetChatReadMarker(chatJID string, t time.Time) error {
	var marker sql.NullTime
	if !t.IsZero() {
		marker = sql.NullTime{Time: t, Valid: true}
	}
	_, err := store.db.Exec(`UPDATE chats SET last_read_timestamp = ? WHERE jid = ?`, marker, chatJID)
	return err
}

// Only sets the marker if it is currently unset, so it never overwrites real unread state.
func (store *MessageStore) seedReadMarkerIfUnset(chatJID string, t time.Time) error {
	_, err := store.db.Exec(
		`UPDATE chats SET last_read_timestamp = ? WHERE jid = ? AND last_read_timestamp IS NULL`,
		t, chatJID,
	)
	return err
}

// Returns the zero Time if there are no incoming messages.
func (store *MessageStore) newestIncomingTimestamp(chatJID string) (time.Time, error) {
	var ts sql.NullTime
	err := store.db.QueryRow(
		`SELECT MAX(timestamp) FROM messages WHERE chat_jid = ? AND is_from_me = 0`,
		chatJID,
	).Scan(&ts)
	if err != nil {
		return time.Time{}, err
	}
	if !ts.Valid {
		return time.Time{}, nil
	}
	return ts.Time, nil
}

// UnreadMessage is one row of an unread-messages query.
type UnreadMessage struct {
	ID        string
	Sender    string
	Timestamp time.Time
}

// A NULL marker means nothing has ever been marked read, so everything is unread.
func (store *MessageStore) GetUnreadMessages(chatJID string) ([]UnreadMessage, error) {
	rows, err := store.db.Query(
		`SELECT m.id, m.sender, m.timestamp
		 FROM messages m JOIN chats c ON c.jid = m.chat_jid
		 WHERE m.chat_jid = ? AND m.is_from_me = 0
		   AND (c.last_read_timestamp IS NULL OR julianday(m.timestamp) > julianday(c.last_read_timestamp))
		 ORDER BY m.timestamp ASC`,
		chatJID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var unread []UnreadMessage
	for rows.Next() {
		var u UnreadMessage
		if err := rows.Scan(&u.ID, &u.Sender, &u.Timestamp); err != nil {
			return nil, err
		}
		unread = append(unread, u)
	}
	return unread, rows.Err()
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT OR REPLACE INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength,
	)
	return err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// For now, we're ignoring non-text messages
	return ""
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
	// VoiceNote sends the media as a playable WhatsApp voice message,
	// transcoding it to Ogg Opus with ffmpeg first if it isn't one already.
	VoiceNote bool `json:"voice_note,omitempty"`
	// uploadName is the original filename of a multipart upload, used as the
	// document title instead of the temp file's name.
	uploadName string
}

// Function to send a WhatsApp message
func sendWhatsAppMessage(ctx context.Context, client *whatsmeow.Client, messageStore *MessageStore, recipient, message, mediaPath string, voiceNote bool, logger waLog.Logger) (bool, string) {
	return sendWhatsAppMedia(ctx, client, messageStore, recipient, message, mediaPath, "", voiceNote, logger)
}

// sendWhatsAppMedia is sendWhatsAppMessage with an explicit document title.
func sendWhatsAppMedia(ctx context.Context, client *whatsmeow.Client, messageStore *MessageStore, recipient, message, mediaPath, title string, voiceNote bool, logger waLog.Logger) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	// Create JID for recipient
	var recipientJID types.JID
	var err error

	// Check if recipient is a JID
	isJID := strings.Contains(recipient, "@")

	if isJID {
		// Parse the JID string
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		// Create JID from phone number
		recipientJID = types.JID{
			User:   recipient,
			Server: "s.whatsapp.net", // For personal chats
		}
	}

	msg := &waProto.Message{}

	// Check if we have media to send
	if mediaPath != "" {
		resolvedPath, err := containExistingPath(messageStore.StoreDir, mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Rejected media_path: %v", err)
		}

		sendPath := resolvedPath
		if voiceNote && !isOggOpus(resolvedPath) {
			converted, err := convertToOpusOgg(ctx, resolvedPath, filepath.Join(messageStore.StoreDir, "tmp"))
			if err != nil {
				return false, fmt.Sprintf("Error converting audio to Ogg Opus: %v", err)
			}
			defer os.Remove(converted)
			sendPath = converted
		}

		mediaData, err := os.ReadFile(sendPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}

		// Determine media type and mime type based on file extension
		fileExt := strings.TrimPrefix(strings.ToLower(filepath.Ext(sendPath)), ".")
		var mediaType whatsmeow.MediaType
		var mimeType string

		// Handle different media types
		switch fileExt {
		// Image types
		case "jpg", "jpeg":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/jpeg"
		case "png":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/png"
		case "gif":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/gif"
		case "webp":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/webp"

		// Audio types
		case "ogg":
			mediaType = whatsmeow.MediaAudio
			mimeType = "audio/ogg; codecs=opus"

		// Video types
		case "mp4":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/mp4"
		case "avi":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/avi"
		case "mov":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/quicktime"

		// Document types (for any other file type)
		default:
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/octet-stream"
		}

		// Upload media to WhatsApp servers
		resp, err := client.Upload(ctx, mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}

		fmt.Println("Media uploaded", resp)

		// Create the appropriate message type based on media type
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			// Handle ogg audio files
			var seconds uint32 = 30 // Default fallback
			var waveform []byte = nil

			// Try to analyze the ogg file
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds = analyzedSeconds
					waveform = analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			} else {
				fmt.Printf("Not an Ogg Opus file: %s\n", mimeType)
			}

			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(true),
				Waveform:      waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			msg.DocumentMessage = &waProto.DocumentMessage{
				Title:         proto.String(documentTitle(title, mediaPath)),
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		}
	} else {
		msg.Conversation = proto.String(message)
	}

	// Send message
	resp, err := client.SendMessage(ctx, recipientJID, msg)

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	// whatsmeow only loops a message back through the event handler (which is
	// what handleMessage/StoreMessage normally run from) for messages arriving
	// from someone else or synced from another of our own linked devices - a
	// message this connection just sent gets no such event. Mirror it into the
	// store here, or it delivers but never appears in list_messages/get_chat.
	chatJID := recipientJID.String()
	mediaType, filename, mediaURL, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg)
	sender := ""
	if client.Store.ID != nil {
		sender = client.Store.ID.User
	}
	name := GetChatName(client, messageStore, recipientJID, chatJID, nil, recipientJID.User, logger)
	if err := messageStore.StoreChat(chatJID, name, resp.Timestamp); err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}
	if err := messageStore.StoreMessage(resp.ID, chatJID, sender, message, resp.Timestamp, true,
		mediaType, filename, mediaURL, mediaKey, fileSHA256, fileEncSHA256, fileLength); err != nil {
		logger.Warnf("Failed to store sent message: %v", err)
	}

	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// documentTitle prefers an explicit title (an upload's original name) over the on-disk filename.
func documentTitle(title, mediaPath string) string {
	if title != "" {
		return title
	}
	return filepath.Base(mediaPath)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, pub *Publisher, transcriber *Transcriber, msg *events.Message, logger waLog.Logger, logBodies bool) {
	// Save message to database. Resolve LID addressing to phone numbers
	// where whatsmeow's local mapping store lets us, so a contact ends up
	// under one chat JID regardless of which addressing form WhatsApp used
	// for a given message.
	ctx := context.Background()
	chat := canonicalChatJID(ctx, client.Store, msg.Info.MessageSource)
	chatJID := chat.String()
	sender := canonicalSenderJID(ctx, client.Store, msg.Info.MessageSource, chat).User

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, chat, chatJID, nil, sender, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Store message in database
	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
		return
	}

	// Voice notes wait for their transcript before publication, so every
	// consumer sees one event with the text attached rather than two.
	if pub != nil {
		switch {
		case mediaType == "audio" && transcriber.Enabled():
			if err := messageStore.markTranscriptionPending(chatJID, msg.Info.ID); err != nil {
				logger.Warnf("Failed to mark %s pending transcription: %v", msg.Info.ID, err)
			}
			transcriber.Enqueue(transcribeJob{chatJID: chatJID, messageID: msg.Info.ID})
		default:
			ev := WebhookEvent{
				MessageID: msg.Info.ID,
				ChatJID:   chatJID,
				ChatName:  name,
				Sender:    sender,
				Content:   content,
				Timestamp: msg.Info.Timestamp.Format(time.RFC3339),
				IsFromMe:  msg.Info.IsFromMe,
				MediaType: mediaType,
				Filename:  filename,
				HasMedia:  mediaType != "",
			}
			if mediaType == "audio" {
				ev.TranscriptionStatus = transcriptionDisabled
				if err := messageStore.setTranscription(chatJID, msg.Info.ID, "", transcriptionDisabled); err != nil {
					logger.Warnf("Failed to record transcription state: %v", err)
				}
			}
			pub.PublishMessage(ev)
		}
	}

	// Log message reception. Body content is only logged when explicitly
	// requested: it ends up in journald, and journald ends up in backups.
	timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
	direction := "←"
	if msg.Info.IsFromMe {
		direction = "→"
	}

	switch {
	case mediaType != "" && logBodies:
		fmt.Printf("[%s] %s %s (%s): [%s: %s] %s\n", timestamp, direction, sender, chatJID, mediaType, filename, content)
	case mediaType != "":
		fmt.Printf("[%s] %s %s (%s): [%s: %s]\n", timestamp, direction, sender, chatJID, mediaType, filename)
	case content != "" && logBodies:
		fmt.Printf("[%s] %s %s (%s): %s\n", timestamp, direction, sender, chatJID, content)
	case content != "":
		fmt.Printf("[%s] %s %s (%s): [text message]\n", timestamp, direction, sender, chatJID)
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Function to download media from a message
func downloadMedia(ctx context.Context, client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	storeDir := messageStore.StoreDir

	// Query the database for the message
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	chatDir := filepath.Join(storeDir, strings.ReplaceAll(chatJID, ":", "_"))

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT media_type, filename FROM messages WHERE id = ? AND chat_jid = ?",
			messageID, chatJID,
		).Scan(&mediaType, &filename)

		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// The filename comes from the remote sender (e.g. a document's declared
	// name) so it must not be trusted to build a filesystem path directly.
	safeName, err := sanitizeMediaFilename(filename)
	if err != nil {
		return false, "", "", "", fmt.Errorf("invalid media filename: %v", err)
	}

	localPath := filepath.Join(chatDir, safeName)
	absPath, err := containPath(storeDir, localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("refusing to write outside store directory: %v", err)
	}

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(absPath); err == nil {
		return true, mediaType, safeName, absPath, nil
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client
	mediaData, err := client.Download(ctx, downloader)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save the downloaded media to file
	if err := os.WriteFile(absPath, mediaData, 0644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, safeName, absPath, nil
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// The direct path is typically in the URL, we need to extract it
	// Example URL: https://mmg.whatsapp.net/v/t62.7118-24/13812002_698058036224062_3424455886509161511_n.enc?ccb=11-4&oh=...

	// Find the path part after the domain
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url // Return original URL if parsing fails
	}

	pathPart := parts[1]

	// Remove query parameters
	pathPart = strings.SplitN(pathPart, "?", 2)[0]

	// Create proper direct path format
	return "/" + pathPart
}

// requireReady writes a 503 JSON error and returns false unless the client is
// both connected and logged in. The REST server is started before pairing so
// callers can poll status, but send/download only make sense once paired.
func requireReady(client *whatsmeow.Client, w http.ResponseWriter) bool {
	if client.IsConnected() && client.Store.ID != nil {
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(map[string]string{
		"error": "WhatsApp bridge is not connected and logged in yet",
	})
	return false
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, pub *Publisher, transcriber *Transcriber, addr, socketGroup string, logBodies bool, logger waLog.Logger) *http.Server {
	mux := http.NewServeMux()
	dispatcher := pub.dispatcher

	registerWebhookRoutes(mux, messageStore, dispatcher, logger)
	registerReadRoutes(mux, messageStore)
	registerEventRoutes(mux, messageStore, pub)
	registerTranscribeRoutes(mux, messageStore, transcriber)

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		jid := ""
		loggedIn := client.Store.ID != nil
		if loggedIn {
			jid = client.Store.ID.String()
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"connected": client.IsConnected(),
			"logged_in": loggedIn,
			"jid":       jid,
		})
	})

	// Handler for sending messages
	mux.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !requireReady(client, w) {
			return
		}

		req, cleanup, err := parseSendRequest(r, messageStore.StoreDir)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer cleanup()

		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}

		if logBodies {
			fmt.Println("Received request to send message", req.Message, req.MediaPath)
		} else {
			fmt.Println("Received request to send message to", req.Recipient)
		}

		success, message := sendWhatsAppMedia(r.Context(), client, messageStore, req.Recipient, req.Message, req.MediaPath, req.uploadName, req.VoiceNote, logger)
		fmt.Println("Message sent", success, message)

		w.Header().Set("Content-Type", "application/json")
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(SendMessageResponse{
			Success: success,
			Message: message,
		})
	})

	// Handler for downloading media
	mux.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !requireReady(client, w) {
			return
		}

		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		success, mediaType, filename, path, err := downloadMedia(r.Context(), client, messageStore, req.MessageID, req.ChatJID)

		w.Header().Set("Content-Type", "application/json")

		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})

	// Streams a media message's bytes, downloading it into the store first if
	// needed, so a client without store access can still fetch attachments.
	mux.HandleFunc("/api/media/{chat_jid}/{message_id}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !requireReady(client, w) {
			return
		}
		success, _, filename, path, err := downloadMedia(r.Context(), client, messageStore, r.PathValue("message_id"), r.PathValue("chat_jid"))
		if !success || err != nil {
			status := http.StatusInternalServerError
			if err != nil && (strings.Contains(err.Error(), "failed to find message") || strings.Contains(err.Error(), "not a media message")) {
				status = http.StatusNotFound
			}
			writeError(w, status, "failed to fetch media: %v", err)
			return
		}
		w.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", filename))
		http.ServeFile(w, r, path)
	})

	// Handler for requesting more on-demand history for one chat
	mux.HandleFunc("/api/resync", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !requireReady(client, w) {
			return
		}

		var req ResyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		lastKnown, count, err := parseResyncRequest(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		fmt.Printf("Requesting %d messages of history for %s before %s\n", count, lastKnown.Chat, lastKnown.ID)

		historyMsg := client.BuildHistorySyncRequest(lastKnown, count)
		_, err = client.SendPeerMessage(r.Context(), historyMsg)

		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ResyncResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to request history sync: %v", err),
			})
			return
		}

		json.NewEncoder(w).Encode(ResyncResponse{
			Success: true,
			Message: fmt.Sprintf("Requested up to %d messages before %s in %s; they arrive as an on-demand history sync event", count, lastKnown.ID, lastKnown.Chat),
		})
	})

	// The only place read state changes; the receive path never marks read.
	mux.HandleFunc("/api/mark-read", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !requireReady(client, w) {
			return
		}

		var req MarkReadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		if req.ChatJID == "" {
			http.Error(w, "chat_jid is required", http.StatusBadRequest)
			return
		}
		// types.ParseJID barely validates, so malformed input is caught on User rather than err alone.
		chat, err := types.ParseJID(req.ChatJID)
		if err != nil || chat.User == "" {
			http.Error(w, fmt.Sprintf("invalid chat_jid: %q", req.ChatJID), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		unread, err := messageStore.GetUnreadMessages(req.ChatJID)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(MarkReadResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to look up unread messages: %v", err),
			})
			return
		}

		if len(unread) == 0 {
			json.NewEncoder(w).Encode(MarkReadResponse{
				Success:     true,
				Message:     "chat already has no unread messages",
				MarkedCount: 0,
				ReceiptSent: false,
			})
			return
		}

		receiptType := types.ReceiptTypeReadSelf
		if req.SendReceipt {
			receiptType = types.ReceiptTypeRead
		}

		order, bySender := groupUnreadBySender(unread)
		var receiptErr error
		for _, sender := range order {
			senderJID := senderJIDForMarkRead(chat, sender)
			if err := client.MarkRead(r.Context(), bySender[sender], time.Now(), chat, senderJID, receiptType); err != nil {
				fmt.Printf("Failed to send read receipt for %d message(s) from %s in %s: %v\n", len(bySender[sender]), senderJID, chat, err)
				if receiptErr == nil {
					receiptErr = err
				}
			}
		}

		// Always advance the marker: a failed receipt must not leave the chat stuck unread.
		newest := unread[len(unread)-1].Timestamp
		if err := messageStore.MarkChatRead(req.ChatJID, newest); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(MarkReadResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to update read marker: %v", err),
			})
			return
		}
		pub.PublishChatRead(req.ChatJID, newest, "api")

		resp := MarkReadResponse{
			Success:     true,
			MarkedCount: len(unread),
			ReceiptSent: req.SendReceipt && receiptErr == nil,
		}
		if receiptErr != nil {
			resp.Message = fmt.Sprintf("Marked %d message(s) read, but sending the read receipt failed: %v", len(unread), receiptErr)
		} else {
			resp.Message = fmt.Sprintf("Marked %d message(s) read", len(unread))
		}
		fmt.Printf("Marked %d message(s) read in %s (send_receipt=%v, receipt_sent=%v)\n", len(unread), req.ChatJID, req.SendReceipt, resp.ReceiptSent)
		json.NewEncoder(w).Encode(resp)
	})

	server := &http.Server{Handler: mux}
	ln, err := listenAddr(addr, socketGroup)
	if err != nil {
		logger.Errorf("Failed to listen on %s: %v", addr, err)
		os.Exit(1)
	}
	fmt.Printf("Starting REST API server on %s...\n", addr)

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()

	return server
}

func main() {
	ctx := context.Background()

	storeDir := getEnvOrDefault("WHATSAPP_STORE_DIR", "store")
	bridgeAddr := getEnvOrDefault("WHATSAPP_BRIDGE_ADDR", "127.0.0.1:8080")
	socketGroup := os.Getenv("WHATSAPP_BRIDGE_SOCKET_GROUP")
	logBodies := os.Getenv("WHATSAPP_LOG_MESSAGE_BODIES") == "1"

	// Set up logger
	logger := waLog.Stdout("Client", "INFO", true)
	logger.Infof("Starting WhatsApp client...")

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	if err := os.MkdirAll(storeDir, 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		os.Exit(1)
	}

	sessionDBPath := filepath.Join(storeDir, "whatsapp.db")
	container, err := sqlstore.New(ctx, "sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on", sessionDBPath), dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		os.Exit(1)
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(ctx)
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			os.Exit(1)
		}
	}

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		os.Exit(1)
	}

	// Initialize message store
	messageStore, err := NewMessageStore(storeDir)
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		os.Exit(1)
	}
	defer messageStore.Close()

	// Every publishable event goes through pub: into the event log, out to
	// SSE subscribers, and (new messages only) to subscribed webhooks.
	dispatcher := NewWebhookDispatcher(messageStore, logger)
	pub := NewPublisher(messageStore, dispatcher, logger)

	// Voice notes are transcribed locally before publication (see transcribe.go).
	transcriber := NewTranscriber(newTranscriberConfigFromEnv(storeDir), messageStore, pub, logger)
	transcriber.fetch = func(ctx context.Context, chatJID, messageID string) (string, error) {
		ok, _, _, path, err := downloadMedia(ctx, client, messageStore, messageID, chatJID)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", fmt.Errorf("download failed")
		}
		return path, nil
	}
	if transcriber.Enabled() {
		logger.Infof("Voice-note transcription enabled (model %s, timeout %s)", transcriber.cfg.ModelPath, transcriber.cfg.Timeout)
		transcriber.Start()
	}

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			handleMessage(client, messageStore, pub, transcriber, v, logger, logBodies)

		case *events.HistorySync:
			handleHistorySync(client, messageStore, v, logger, logBodies)

		case *events.Receipt:
			handleReceipt(client, messageStore, pub, v, logger)

		case *events.MarkChatAsRead:
			handleMarkChatAsRead(client, messageStore, pub, v, logger)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")
			jid := ""
			if client.Store.ID != nil {
				jid = client.Store.ID.String()
			}
			pub.PublishBridgeStatus(true, client.Store.ID != nil, jid)

		case *events.Disconnected:
			logger.Warnf("Disconnected from WhatsApp")
			pub.PublishBridgeStatus(false, client.Store.ID != nil, "")

		case *events.LoggedOut:
			logger.Warnf("Device logged out, please scan QR code to log in again")
			pub.PublishBridgeStatus(client.IsConnected(), false, "")
		}
	})

	// The REST server (and /api/status in particular) must be answerable
	// before pairing completes, so start it before the QR/connect phase.
	server := startRESTServer(client, messageStore, pub, transcriber, bridgeAddr, socketGroup, logBodies, logger)
	defer server.Close()

	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		qrChan, err := client.GetQRChannel(ctx)
		if err != nil {
			logger.Errorf("Failed to get QR channel: %v", err)
			os.Exit(1)
		}
		if err := client.Connect(); err != nil {
			logger.Errorf("Failed to connect: %v", err)
			os.Exit(1)
		}

		// whatsmeow rotates the QR code itself until it is scanned or the
		// pairing is abandoned, so there is no client-side timeout here:
		// on a headless box, systemd should just restart-loop instead.
		success := false
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(pairingPayload(evt.Code), qrterminal.L, os.Stdout)
			case "success":
				success = true
			default:
				logger.Infof("QR channel event: %s", evt.Event)
			}
		}
		if !success {
			logger.Errorf("QR channel closed without a successful pairing")
			os.Exit(1)
		}
		fmt.Println("\nSuccessfully connected and authenticated!")
	} else {
		// Already logged in, just connect
		if err := client.Connect(); err != nil {
			logger.Errorf("Failed to connect: %v", err)
			os.Exit(1)
		}
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		os.Exit(1)
	}

	fmt.Println("\n✓ Connected to WhatsApp!")

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Wait for termination signal
	<-exitChan

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	ctx := context.Background()

	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Debugf("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(ctx, jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Just use contact info (full name)
		contact, err := client.Store.Contacts.GetContact(ctx, jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			// Fallback to sender
			name = sender
		} else {
			// Last fallback to JID
			name = jid.User
		}

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger, logBodies bool) {
	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	ctx := context.Background()
	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		// Try to parse the JID
		jid, err := types.ParseJID(*conversation.ID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", *conversation.ID, err)
			continue
		}

		// Resolve LID addressing the same way handleMessage does, so a
		// history-synced chat lands in the same row as one seen live.
		jid = canonicalHistorySyncChatJID(ctx, client.Store, jid, conversation.GetPnJID())
		chatJID := jid.String()

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Seed only on initial syncs; ON_DEMAND resyncs pull older history and must not touch the marker.
			if historySync.Data.GetSyncType() != waHistorySync.HistorySync_ON_DEMAND {
				if err := messageStore.seedReadMarkerIfUnset(chatJID, timestamp); err != nil {
					logger.Warnf("Failed to seed read marker for %s: %v", chatJID, err)
				}
			}

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				var content string
				if msg.Message.Message != nil {
					if conv := msg.Message.Message.GetConversation(); conv != "" {
						content = conv
					} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
						content = ext.GetText()
					}
				}

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}

				if logBodies {
					logger.Infof("Message content: %v, Media Type: %v", content, mediaType)
				} else {
					logger.Debugf("Synced message metadata: media_type=%v", mediaType)
				}

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender. jid is already the resolved canonical
				// chat JID; a group participant is resolved the same way
				// canonicalSenderJID resolves a live one, but history sync
				// carries no per-message alt address, so this can only fall
				// back to the resolver's local cache (see lid.go).
				var sender string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					switch {
					case !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "":
						if participant, err := types.ParseJID(*msg.Message.Key.Participant); err == nil {
							sender = resolveAlt(ctx, client.Store, participant, types.JID{}).User
						} else {
							sender = *msg.Message.Key.Participant
						}
					case isFromMe:
						sender = resolveAlt(ctx, client.Store, *client.Store.ID, types.JID{}).User
					default:
						sender = jid.User
					}
				} else {
					sender = jid.User
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
					continue
				}

				syncedCount++
				if mediaType != "" && logBodies {
					logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
						timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
				} else if mediaType != "" {
					logger.Infof("Stored message: [%s] %s -> %s: [%s: %s]",
						timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename)
				} else if logBodies {
					logger.Infof("Stored message: [%s] %s -> %s: %s",
						timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
				} else {
					logger.Infof("Stored message: [%s] %s -> %s",
						timestamp.Format("2006-01-02 15:04:05"), sender, chatJID)
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	// Try to detect if this is a valid Ogg file by checking for the "OggS" signature
	// at the beginning of the file
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	// Parse Ogg pages to find the last page with a valid granule position
	var lastGranule uint64
	var sampleRate uint32 = 48000 // Default Opus sample rate
	var preSkip uint16 = 0
	var foundOpusHead bool

	// Scan through the file looking for Ogg pages
	for i := 0; i < len(data); {
		// Check if we have enough data to read Ogg page header
		if i+27 >= len(data) {
			break
		}

		// Verify Ogg page signature
		if string(data[i:i+4]) != "OggS" {
			// Skip until next potential page
			i++
			continue
		}

		// Extract header fields
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])

		// Extract segment table
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Calculate page size
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}

		// Check if we're looking at an OpusHead packet (should be in first few pages)
		if !foundOpusHead && pageSeqNum <= 1 {
			// Look for "OpusHead" marker in this page
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				// Found OpusHead, extract sample rate and pre-skip
				// OpusHead format: Magic(8) + Version(1) + Channels(1) + PreSkip(2) + SampleRate(4) + ...
				headPos += 8 // Skip "OpusHead" marker
				// PreSkip is 2 bytes at offset 10
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
					fmt.Printf("Found OpusHead: sampleRate=%d, preSkip=%d\n", sampleRate, preSkip)
				}
			}
		}

		// Keep track of last valid granule position
		if granulePos != 0 {
			lastGranule = granulePos
		}

		// Move to next page
		i += pageSize
	}

	if !foundOpusHead {
		fmt.Println("Warning: OpusHead not found, using default values")
	}

	// Calculate duration based on granule position
	if lastGranule > 0 {
		// Formula for duration: (lastGranule - preSkip) / sampleRate
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
		fmt.Printf("Calculated Opus duration from granule: %f seconds (lastGranule=%d)\n",
			durationSeconds, lastGranule)
	} else {
		// Fallback to rough estimation if granule position not found
		fmt.Println("Warning: No valid granule position found, using estimation")
		durationEstimate := float64(len(data)) / 2000.0 // Very rough approximation
		duration = uint32(durationEstimate)
	}

	// Make sure we have a reasonable duration (at least 1 second, at most 300 seconds)
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}

	// Generate waveform
	waveform = placeholderWaveform(duration)

	fmt.Printf("Ogg Opus analysis: size=%d bytes, calculated duration=%d sec, waveform=%d bytes\n",
		len(data), duration, len(waveform))

	return duration, waveform, nil
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
// that appears natural with some variability based on the duration
func placeholderWaveform(duration uint32) []byte {
	// WhatsApp expects a 64-byte waveform for voice messages
	const waveformLength = 64
	waveform := make([]byte, waveformLength)

	// Seed the random number generator for consistent results with the same duration
	rand.Seed(int64(duration))

	// Create a more natural looking waveform with some patterns and variability
	// rather than completely random values

	// Base amplitude and frequency - longer messages get faster frequency
	baseAmplitude := 35.0
	frequencyFactor := float64(min(int(duration), 120)) / 30.0

	for i := range waveform {
		// Position in the waveform (normalized 0-1)
		pos := float64(i) / float64(waveformLength)

		// Create a wave pattern with some randomness
		// Use multiple sine waves of different frequencies for more natural look
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)

		// Add some randomness to make it look more natural
		val += (rand.Float64() - 0.5) * 15

		// Add some fade-in and fade-out effects
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)

		// Center around 50 (typical voice baseline)
		val = val + 50

		// Ensure values stay within WhatsApp's expected range (0-100)
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}

		waveform[i] = byte(val)
	}

	return waveform
}
