package repositories

import "testing"

func TestParseSnowflake(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr bool
	}{
		{"discord id", "1301881978244456458", 1301881978244456458, false},
		{"zero", "0", 0, false},
		{"empty", "", 0, true},
		{"not a number", "abc", 0, true},
		{"float", "1.5", 0, true},
		{"negative", "-1", 0, true},
		{"overflow", "99999999999999999999", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSnowflake(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseSnowflake(%q) expected error, got %d", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSnowflake(%q) unexpected error: %v", tt.raw, err)
			}
			if got != tt.want {
				t.Errorf("parseSnowflake(%q) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}
