package main

import (
	"strings"
	"testing"
)

func normalizeToken(raw string) string {
	token := strings.TrimSpace(raw)
	token = strings.Trim(token, `"'`)
	token = strings.TrimSpace(token)
	if token == "" || token == "Bot" {
		return ""
	}
	token = strings.TrimPrefix(token, "Bot ")
	token = strings.TrimSpace(token)
	token = strings.Trim(token, `"'`)
	return strings.TrimSpace(token)
}

func TestNormalizeToken(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"plain", "abc.def.ghi", "abc.def.ghi"},
		{"surrounding whitespace", "  abc.def.ghi  ", "abc.def.ghi"},
		{"crlf", "abc.def.ghi\r\n", "abc.def.ghi"},
		{"double quotes", `"abc.def.ghi"`, "abc.def.ghi"},
		{"single quotes", "'abc.def.ghi'", "abc.def.ghi"},
		{"quoted with whitespace", ` "abc.def.ghi" `, "abc.def.ghi"},
		{"bot prefix", "Bot abc.def.ghi", "abc.def.ghi"},
		{"bot prefix with quoted token", `Bot "abc.def.ghi"`, "abc.def.ghi"},
		{"quoted bot prefix", `"Bot abc.def.ghi"`, "abc.def.ghi"},
		{"empty", "", ""},
		{"whitespace only", " \t\r\n ", ""},
		{"empty quotes", `""`, ""},
		{"prefix only", "Bot ", ""},
		{"prefix without separator", "Bot", ""},
		{"token starting with Bot", "Botanical.token.here", "Botanical.token.here"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeToken(tt.raw); got != tt.want {
				t.Errorf("normalizeToken(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
