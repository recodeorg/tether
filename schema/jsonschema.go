package schema

// JSONSchemaDialect is the "$schema" URI of the documents [JSONSchema]
// returns.
const JSONSchemaDialect = "https://json-schema.org/draft/2020-12/schema"

// JSONSchema returns s as a JSON Schema (draft 2020-12) document, ready for
// encoding/json. It describes what a client should send: the keys and types
// of every field, which fields are required, and the built-in checks such as
// Min, Email and Strict. Describe sets "description" and Default sets
// "default".
//
//	b, _ := json.Marshal(schema.JSONSchema(createMessage))
//
// The export is a close description, not an exact one:
//
//   - Refine functions cannot be exported, so Validate may reject a value the
//     JSON Schema accepts.
//   - String checks describe the string after Trim, ToLower and ToUpper.
//   - Regex exports the pattern in Go's RE2 syntax, which JSON Schema tools
//     read as ECMAScript. Common patterns mean the same in both.
//   - Email, URL, UUID and Datetime export as "format", which many
//     validators treat as a note rather than a check.
//
// Each call returns a new map, which the caller may modify.
func JSONSchema(s Schema) map[string]any {
	out := toJSON(s)
	out["$schema"] = JSONSchemaDialect
	return out
}

// toJSON returns s's JSON Schema with its modifiers applied, without
// "$schema".
func toJSON(s Schema) map[string]any {
	out := s.jsonSchema()
	m := s.modifiers()
	if m.nullable {
		allowNull(out)
	}
	if m.description != "" {
		out["description"] = m.description
	}
	if m.hasDefault {
		out["default"] = copyValue(m.def)
	}
	return out
}

// allowNull changes out to also accept null.
func allowNull(out map[string]any) {
	if t, ok := out["type"].(string); ok {
		out["type"] = []any{t, "null"}
	}
	if c, ok := out["const"]; ok {
		delete(out, "const")
		out["enum"] = []any{c, nil}
	} else if e, ok := out["enum"].([]any); ok {
		out["enum"] = append(e, nil)
	}
	if a, ok := out["anyOf"].([]any); ok {
		out["anyOf"] = append(a, map[string]any{"type": "null"})
	}
}

// addChecks adds the keyword of each check to out. A keyword out already
// has, such as a second pattern, goes in "allOf" so both still apply.
func addChecks[T any](out map[string]any, checks []check[T]) {
	var all []any
	for _, c := range checks {
		if c.kw == "" {
			continue
		}
		if _, dup := out[c.kw]; dup {
			all = append(all, map[string]any{c.kw: c.arg})
			continue
		}
		out[c.kw] = c.arg
	}
	if len(all) > 0 {
		out["allOf"] = all
	}
}

func (s *StringSchema) jsonSchema() map[string]any {
	out := map[string]any{"type": "string"}
	addChecks(out, s.checks)
	return out
}

func (s *NumberSchema) jsonSchema() map[string]any {
	out := map[string]any{"type": "number"}
	addChecks(out, s.checks)
	return out
}

func (s *IntSchema) jsonSchema() map[string]any {
	out := map[string]any{"type": "integer"}
	addChecks(out, s.checks)
	return out
}

func (s *BoolSchema) jsonSchema() map[string]any {
	return map[string]any{"type": "boolean"}
}

func (s *LiteralSchema) jsonSchema() map[string]any {
	return map[string]any{"const": s.value}
}

func (s *EnumSchema) jsonSchema() map[string]any {
	values := make([]any, len(s.values))
	for i, v := range s.values {
		values[i] = v
	}
	return map[string]any{"type": "string", "enum": values}
}

// jsonSchema returns the empty schema, which accepts every value.
func (s *AnySchema) jsonSchema() map[string]any {
	return map[string]any{}
}

func (s *ArraySchema) jsonSchema() map[string]any {
	out := map[string]any{"type": "array", "items": toJSON(s.item)}
	addChecks(out, s.checks)
	return out
}

// jsonSchema lists every field under "properties". A field is required
// unless it is Optional or has a Default. Only Strict objects set
// "additionalProperties": the default Strip accepts unknown keys and
// removes them.
func (s *ObjectSchema) jsonSchema() map[string]any {
	props := make(map[string]any, len(s.shape))
	required := []any{}
	for _, k := range s.keys {
		f := s.shape[k]
		props[k] = toJSON(f)
		if m := f.modifiers(); !m.optional && !m.hasDefault {
			required = append(required, k)
		}
	}
	out := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		out["required"] = required
	}
	if s.unknown == rejectUnknown {
		out["additionalProperties"] = false
	}
	return out
}

func (s *RecordSchema) jsonSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": toJSON(s.value)}
}

func (s *UnionSchema) jsonSchema() map[string]any {
	options := make([]any, len(s.options))
	for i, o := range s.options {
		options[i] = toJSON(o)
	}
	return map[string]any{"anyOf": options}
}
