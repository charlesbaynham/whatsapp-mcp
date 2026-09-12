package main

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"strconv"
	"strings"
)

// listenAddr opens the bridge's listener. addr is either a TCP host:port or
// "unix:/path/to.sock". A Unix socket is the intended deployment: nothing
// listens on TCP, and whoever can open the socket file has full access, so
// its mode is forced to 0660 and, if socketGroup is set, its group is
// changed so client units can be admitted by group membership alone.
func listenAddr(addr, socketGroup string) (net.Listener, error) {
	path, isUnix := strings.CutPrefix(addr, "unix:")
	if !isUnix {
		return net.Listen("tcp", addr)
	}
	if path == "" {
		return nil, fmt.Errorf("unix socket path is empty")
	}
	// A stale socket from a previous run refuses the bind; only a socket is
	// ever removed here, never a regular file at the same path.
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("removing stale socket: %w", err)
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		ln.Close()
		return nil, fmt.Errorf("chmod socket: %w", err)
	}
	if socketGroup != "" {
		grp, err := user.LookupGroup(socketGroup)
		if err != nil {
			ln.Close()
			return nil, fmt.Errorf("looking up socket group %q: %w", socketGroup, err)
		}
		gid, err := strconv.Atoi(grp.Gid)
		if err != nil {
			ln.Close()
			return nil, fmt.Errorf("parsing gid for %q: %w", socketGroup, err)
		}
		if err := os.Chown(path, -1, gid); err != nil {
			ln.Close()
			return nil, fmt.Errorf("chgrp socket to %q: %w", socketGroup, err)
		}
	}
	return ln, nil
}
