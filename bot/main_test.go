package main

import "testing"

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
