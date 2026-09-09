package main

import "testing"

func TestPairingPayload(t *testing.T) {
	const bare = "2@abcDEF+/=,aaaa,bbbb,cccc,1"

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"fragment wrapper", "https://wa.me/settings/linked_devices#" + bare, bare},
		{"query wrapper", "https://wa.me/settings/linked_devices?" + bare, bare},
		{"bare payload untouched", bare, bare},
		{"unrelated url untouched", "https://example.com/x#y", "https://example.com/x#y"},
		{"wa.me without separator untouched", "https://wa.me/settings", "https://wa.me/settings"},
		{"empty", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := pairingPayload(c.in); got != c.want {
				t.Fatalf("pairingPayload(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
