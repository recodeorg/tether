package utilities

const redactedCredential = "[redacted]"

// sensitiveFrameFields are credential keys on client frames. They must not be
// copied into logs or metric names.
var sensitiveFrameFields = map[string]struct{}{
	"token": {},
}

// RedactSensitiveFrame returns a copy of msg with credential fields replaced.
// Nested maps and slices are copied too. The input is left unchanged so
// authentication can still read the original secret.
func RedactSensitiveFrame(msg map[string]interface{}) map[string]interface{} {
	if msg == nil {
		return nil
	}
	out := make(map[string]interface{}, len(msg))
	for k, v := range msg {
		if _, sensitive := sensitiveFrameFields[k]; sensitive {
			out[k] = redactedCredential
			continue
		}
		out[k] = redactSensitiveValue(v)
	}
	return out
}

func redactSensitiveValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		return RedactSensitiveFrame(val)
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, item := range val {
			out[i] = redactSensitiveValue(item)
		}
		return out
	default:
		return v
	}
}
