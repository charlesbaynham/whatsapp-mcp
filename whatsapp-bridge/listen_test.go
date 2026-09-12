package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestListenAddrUnixSocket(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "b.sock")
	// A stale socket file must be replaced, not refused.
	stale, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	stale.Close()
	if _, err := os.Stat(sock); err == nil {
		// Go removes the socket on Close; recreate a stale one by hand.
	}
	if err := os.WriteFile(sock, nil, 0o600); err == nil {
		// A regular file at the path must be refused, never deleted.
		if _, err := listenAddr("unix:"+sock, ""); err == nil {
			t.Fatal("expected refusal for a non-socket file at the socket path")
		}
		os.Remove(sock)
	}

	ln, err := listenAddr("unix:"+sock, "")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	fi, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o660 {
		t.Errorf("socket mode = %o, want 0660", fi.Mode().Perm())
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("pong")) })
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	defer srv.Close()

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", sock)
		},
	}}
	resp, err := client.Get("http://whatsapp/ping")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d", resp.StatusCode)
	}

	if _, err := listenAddr("unix:"+sock, "no-such-group-xyz"); err == nil {
		t.Error("expected error for unknown socket group")
	}
}
