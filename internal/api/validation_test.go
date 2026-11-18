package api

import (
	"encoding/json"
	"testing"
)

func TestInputParsing(t *testing.T) {
	for _, raw := range []string{`0`, `9223372036854775807`} {
		fields := map[string]json.RawMessage{"n": json.RawMessage(raw)}
		if _, ok := intField(fields, "n"); !ok {
			t.Fatal(raw)
		}
	}
	for _, raw := range []string{`null`, `true`, `"0"`, `-1`, `1.0`, `1e2`, `9223372036854775808`} {
		fields := map[string]json.RawMessage{"n": json.RawMessage(raw)}
		if _, ok := intField(fields, "n"); ok {
			t.Fatal(raw)
		}
	}
	for _, raw := range []string{"", "0", "-1", "1.2", "9223372036854775808"} {
		if _, ok := positive(raw, 50); ok {
			t.Fatal(raw)
		}
	}
	if n, ok := positive("50", 50); !ok || n != 50 {
		t.Fatal(n, ok)
	}
}
