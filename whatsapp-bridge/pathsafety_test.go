package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSanitizeMediaFilename(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"plain", "document.pdf", "document.pdf", false},
		{"empty", "", "", true},
		{"dot", ".", "", true},
		{"dotdot", "..", "", true},
		{"traversal", "../../etc/passwd", "passwd", false}, // Base strips the traversal
		{"embedded traversal after base", "..", "", true},
		{"absolute", "/etc/passwd", "passwd", false},
		{"backslash", `a\b`, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := sanitizeMediaFilename(c.in)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got none (result %q)", c.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", c.in, err)
			}
			if got != c.want {
				t.Fatalf("sanitizeMediaFilename(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestContainPath(t *testing.T) {
	base := "/store"
	cases := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{"inside", "/store/chat_1/file.jpg", false},
		{"exact base", "/store", false},
		{"traversal", "/store/../etc/passwd", true},
		{"sibling", "/storeother/file", true},
		{"absolute escape", "/etc/passwd", true},
		{"relative traversal", "chat/../../escape", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := containPath(base, c.target)
			if c.wantErr && err == nil {
				t.Fatalf("containPath(%q, %q) = nil error, want error", base, c.target)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("containPath(%q, %q) = %v, want no error", base, c.target, err)
			}
		})
	}
}

func TestContainExistingPath(t *testing.T) {
	dir := t.TempDir()
	storeDir := filepath.Join(dir, "store")
	if err := os.MkdirAll(storeDir, 0755); err != nil {
		t.Fatal(err)
	}
	insideFile := filepath.Join(storeDir, "media.jpg")
	if err := os.WriteFile(insideFile, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	outsideDir := filepath.Join(dir, "outside")
	if err := os.MkdirAll(outsideDir, 0755); err != nil {
		t.Fatal(err)
	}
	outsideFile := filepath.Join(outsideDir, "secret.txt")
	if err := os.WriteFile(outsideFile, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := containExistingPath(storeDir, insideFile); err != nil {
		t.Fatalf("expected file inside store dir to be allowed, got %v", err)
	}
	if _, err := containExistingPath(storeDir, outsideFile); err == nil {
		t.Fatal("expected file outside store dir to be rejected")
	}
	if _, err := containExistingPath(storeDir, filepath.Join(storeDir, "..", "outside", "secret.txt")); err == nil {
		t.Fatal("expected traversal path to be rejected")
	}

	// Symlink inside the store dir pointing outside it must not be treated as contained.
	symlink := filepath.Join(storeDir, "escape.txt")
	if err := os.Symlink(outsideFile, symlink); err != nil {
		t.Skipf("symlinks not supported in this environment: %v", err)
	}
	if _, err := containExistingPath(storeDir, symlink); err == nil {
		t.Fatal("expected symlink escaping store dir to be rejected")
	}
}
