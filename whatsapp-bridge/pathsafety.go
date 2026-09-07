package main

import (
	"fmt"
	"path/filepath"
	"strings"
)

// sanitizeMediaFilename applies filepath.Base and rejects anything that could
// still be used to escape a directory (empty, ".", "..", or an embedded separator).
func sanitizeMediaFilename(filename string) (string, error) {
	base := filepath.Base(filename)
	if base == "" || base == "." || base == ".." || strings.ContainsAny(base, `/\`) {
		return "", fmt.Errorf("invalid filename %q", filename)
	}
	return base, nil
}

// containPath asserts that target lies inside baseDir, purely lexically (no
// filesystem access), and returns its cleaned absolute form. Use this when
// target does not necessarily exist yet, e.g. a file about to be written.
func containPath(baseDir, target string) (string, error) {
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("resolving base directory: %w", err)
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolving target path: %w", err)
	}
	rel, err := filepath.Rel(absBase, absTarget)
	if err != nil {
		return "", fmt.Errorf("comparing %q against %q: %w", target, baseDir, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes store directory %q", target, baseDir)
	}
	return absTarget, nil
}

// containExistingPath is like containPath but additionally resolves symlinks
// on both sides before comparing, so a symlink cannot be used to point a
// path that lexically looks contained at a file outside baseDir. Use this
// for paths supplied by a caller that must already exist on disk.
func containExistingPath(baseDir, target string) (string, error) {
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("resolving base directory: %w", err)
	}
	resolvedBase, err := filepath.EvalSymlinks(absBase)
	if err != nil {
		return "", fmt.Errorf("resolving base directory: %w", err)
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolving target path: %w", err)
	}
	resolvedTarget, err := filepath.EvalSymlinks(absTarget)
	if err != nil {
		return "", fmt.Errorf("resolving target path: %w", err)
	}
	rel, err := filepath.Rel(resolvedBase, resolvedTarget)
	if err != nil {
		return "", fmt.Errorf("comparing %q against %q: %w", target, baseDir, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes store directory %q", target, baseDir)
	}
	return resolvedTarget, nil
}
