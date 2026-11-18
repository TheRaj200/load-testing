package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"unicode/utf8"
)

func validString(s string, max int) bool {
	return utf8.ValidString(s) && utf8.RuneCountInString(s) >= 1 && utf8.RuneCountInString(s) <= max
}

func readObject(r *http.Request) (map[string]json.RawMessage, error) {
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil || len(b) == 0 {
		return nil, errors.New("empty body")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(b, &fields) != nil || fields == nil {
		return nil, errors.New("invalid object")
	}
	return fields, nil
}

func stringField(fields map[string]json.RawMessage, key string, max int) (string, bool) {
	raw, ok := fields[key]
	if !ok {
		return "", false
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || !validString(value, max) {
		return "", false
	}
	return value, true
}

func intField(fields map[string]json.RawMessage, key string) (int64, bool) {
	raw, ok := fields[key]
	if !ok || len(raw) == 0 {
		return 0, false
	}
	value := string(raw)
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(value, 10, 64)
	return n, err == nil
}

func positive(value string, max int64) (int64, bool) {
	if value == "" {
		return 0, false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(value, 10, 64)
	return n, err == nil && n >= 1 && n <= max
}
