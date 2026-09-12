package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const ffmpegTimeout = 2 * time.Minute

// convertToOpusOgg transcodes any audio file ffmpeg understands into the
// Ogg Opus voice-note format WhatsApp plays inline, writing the result to a
// fresh file under tmpDir. The caller removes it once sent. Settings match
// what the Python server used (32 kb/s, 24 kHz, voip tuning).
func convertToOpusOgg(ctx context.Context, input, tmpDir string) (string, error) {
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return "", fmt.Errorf("creating tmp dir: %w", err)
	}
	out, err := os.CreateTemp(tmpDir, "voice-*.ogg")
	if err != nil {
		return "", fmt.Errorf("creating temp file: %w", err)
	}
	outPath := out.Name()
	out.Close()

	ctx, cancel := context.WithTimeout(ctx, ffmpegTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-i", input,
		"-c:a", "libopus",
		"-b:a", "32k",
		"-ar", "24000",
		"-application", "voip",
		"-vbr", "on",
		"-compression_level", "10",
		"-frame_duration", "60",
		"-y", outPath,
	)
	stderr, err := cmd.CombinedOutput()
	if err != nil {
		os.Remove(outPath)
		tail := string(stderr)
		if len(tail) > 600 {
			tail = tail[len(tail)-600:]
		}
		return "", fmt.Errorf("ffmpeg failed (is it installed?): %v: %s", err, strings.TrimSpace(tail))
	}
	return outPath, nil
}

// isOggOpus reports whether the file already carries the OggS signature, in
// which case no transcode is needed for a voice note.
func isOggOpus(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var sig [4]byte
	if _, err := f.Read(sig[:]); err != nil {
		return false
	}
	return string(sig[:]) == "OggS" && strings.EqualFold(filepath.Ext(path), ".ogg")
}
