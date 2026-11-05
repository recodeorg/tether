package tether

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/recodeorg/tether/schema"
)

// validateParams preserves the schema's error so callers can inspect its issues.
func validateParams(s schema.Schema, params any) (map[string]any, error) {
	validated, err := s.Validate(params)
	if err != nil {
		return nil, err
	}
	out, ok := validated.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("input schema must produce an object")
	}
	return out, nil
}

func validateActionParams(s schema.Schema, r *http.Request) (map[string]any, error) {
	if r.Body == nil {
		return nil, fmt.Errorf("invalid JSON request body: empty body")
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}
	// Keep the original bytes available to handlers, including webhook handlers
	// that verify a signature over the body.
	r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	var params any
	if err := json.Unmarshal(body, &params); err != nil {
		return nil, fmt.Errorf("invalid JSON request body: %w", err)
	}
	return validateParams(s, params)
}
