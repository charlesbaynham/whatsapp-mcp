package main

import "strings"

// whatsmeow emits the pairing payload wrapped in a deep link
// (https://wa.me/settings/linked_devices#<ref>,<keys...>). WhatsApp's in-app
// "Link a Device" scanner silently ignores that form — no error, it just does
// not scan — and accepts only the bare payload, so unwrap it before rendering.
func pairingPayload(code string) string {
	if !strings.HasPrefix(code, "https://wa.me/") {
		return code
	}
	if i := strings.IndexAny(code, "#?"); i >= 0 {
		return code[i+1:]
	}
	return code
}
