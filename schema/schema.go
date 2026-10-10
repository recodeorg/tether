// Package schema builds validators for untrusted values, such as the params
// clients send to queries, mutations, guards and actions.
//
// Schemas are built by chaining methods, in the style of Zod:
//
//	createMessage := schema.Object(schema.Shape{
//	    "roomID": schema.String().UUID(),
//	    "body":   schema.String().Trim().Min(1).Max(2000),
//	    "tags":   schema.Array(schema.Enum("urgent", "fyi")).Max(5).Optional(),
//	    "pinned": schema.Bool().Default(false),
//	})
//
//	params, err := createMessage.Validate(ctx.Params)
//
// Validate returns a new value built from its input: unknown object keys are
// removed, defaults are filled in, and strings are trimmed or case-folded
// when asked. Its error is an [*Error] listing every problem found, not only
// the first.
//
// Values are expected in the form encoding/json decodes them into: string,
// float64, bool, nil, []any and map[string]any. Other Go numbers, slices,
// maps with string keys and named string or bool types are accepted too, so
// params built in server code validate the same way. Output always uses the
// JSON forms, except that [Int] returns int64.
//
// Every method returns a new schema and leaves its receiver unchanged, so a
// schema can be shared and extended safely:
//
//	name := schema.String().Trim().Min(1)
//	nickname := name.Optional() // name is still required
//
// Describe attaches a note for people reading the schema. Description
// returns it. Neither changes what Validate accepts:
//
//	name = name.Describe("display name")
//	name.Description() // "display name"
//
// [Parse] validates a value and decodes the result into a Go type.
//
// [JSONSchema] exports a schema as a JSON Schema document, for clients,
// documentation and code generators.
package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// Schema validates a value. Every schema in this package implements it.
// Schemas cannot be implemented outside this package; use [Any] with Refine
// for a custom check.
type Schema interface {
	// Validate checks v and returns the value it describes. On failure it
	// returns nil and an [*Error] listing every problem found.
	Validate(v any) (any, error)

	// Description returns the text set with Describe, or "" when none was
	// set.
	Description() string

	// modifiers returns the schema's optional, nullable and default
	// settings.
	modifiers() modifiers
	// withModifiers returns a copy of the schema with f applied to its
	// modifiers.
	withModifiers(f func(*modifiers)) Schema
	// parse checks a non-nil v, or nil when the schema accepts null, and
	// returns the output value. Problems are appended to iss with paths
	// under path.
	parse(v any, path []any, iss *[]Issue) any
	// jsonSchema returns the JSON Schema for the schema, without its
	// modifiers. See [JSONSchema].
	jsonSchema() map[string]any
}

// modifiers holds the settings every schema shares.
type modifiers struct {
	// optional lets an object field be missing. The field is then left out
	// of the output.
	optional bool
	// nullable accepts nil and returns it unchanged.
	nullable bool
	// hasDefault fills in def when an object field is missing.
	hasDefault bool
	def        any
	// description is documentation. It does not affect validation.
	description string
}

// run validates v against s, handling nil before s sees it.
func run(s Schema, v any, path []any, iss *[]Issue) any {
	if v == nil && s.modifiers().nullable {
		return nil
	}
	return s.parse(v, path, iss)
}

// validate is the implementation of every schema's Validate method.
func validate(s Schema, v any) (any, error) {
	var iss []Issue
	out := run(s, v, nil, &iss)
	if len(iss) > 0 {
		return nil, &Error{Issues: iss}
	}
	return out, nil
}

// missing returns the value for an object field that is not present, and
// whether the field may be left out of the output.
func missing(s Schema) (value any, omit bool, ok bool) {
	m := s.modifiers()
	switch {
	case m.hasDefault:
		return copyValue(m.def), false, true
	case m.optional:
		return nil, true, true
	}
	return nil, false, false
}

func setOptional(m *modifiers) { m.optional = true }
func setNullable(m *modifiers) { m.nullable = true }

func setDescription(text string) func(*modifiers) {
	return func(m *modifiers) { m.description = text }
}

func setDefault(v any) func(*modifiers) {
	return func(m *modifiers) {
		m.hasDefault = true
		m.def = v
	}
}

// Parse validates v against s and decodes the result into T. If the result
// already has type T it is returned as is. Otherwise it is converted through
// encoding/json, so T may be a struct with json tags:
//
//	type CreateMessage struct {
//	    RoomID string `json:"roomID"`
//	    Body   string `json:"body"`
//	}
//
//	msg, err := schema.Parse[CreateMessage](createMessage, ctx.Params)
func Parse[T any](s Schema, v any) (T, error) {
	var out T
	res, err := s.Validate(v)
	if err != nil {
		return out, err
	}
	if t, ok := res.(T); ok {
		return t, nil
	}
	b, err := json.Marshal(res)
	if err != nil {
		return out, fmt.Errorf("schema: encode validated value: %w", err)
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return out, fmt.Errorf("schema: decode validated value into %T: %w", out, err)
	}
	return out, nil
}

// Code identifies the kind of problem an [Issue] reports.
type Code string

const (
	// CodeInvalidType means the value has the wrong type, including null
	// where null is not allowed.
	CodeInvalidType Code = "invalid_type"
	// CodeRequired means a required object field is missing.
	CodeRequired Code = "required"
	// CodeTooSmall means a number is below its minimum, or a string or
	// array is shorter than its minimum length.
	CodeTooSmall Code = "too_small"
	// CodeTooBig means a number is above its maximum, or a string or array
	// is longer than its maximum length.
	CodeTooBig Code = "too_big"
	// CodeNotInteger means a number has a fractional part or does not fit
	// in an int64.
	CodeNotInteger Code = "not_integer"
	// CodeNotMultipleOf means a number is not a multiple of the required
	// step.
	CodeNotMultipleOf Code = "not_multiple_of"
	// CodeInvalidString means a string does not have the required format,
	// prefix, suffix or pattern.
	CodeInvalidString Code = "invalid_string"
	// CodeInvalidEnum means a string is not one of the allowed values.
	CodeInvalidEnum Code = "invalid_enum"
	// CodeInvalidLiteral means a value is not the one allowed value.
	CodeInvalidLiteral Code = "invalid_literal"
	// CodeInvalidUnion means a value matches none of a union's options.
	CodeInvalidUnion Code = "invalid_union"
	// CodeUnrecognizedKeys means a strict object has keys its shape does
	// not list.
	CodeUnrecognizedKeys Code = "unrecognized_keys"
	// CodeCustom means a Refine function returned an error.
	CodeCustom Code = "custom"
)

// Issue is one problem found by Validate.
type Issue struct {
	// Path leads from the validated value to the one with the problem. Each
	// element is a string object key or an int array index. It is empty
	// when the problem is with the validated value itself.
	Path    []any  `json:"path"`
	Code    Code   `json:"code"`
	Message string `json:"message"`
}

// String returns the issue's path and message, such as
// "members[2].name: must be at least 1 character".
func (i Issue) String() string {
	p := FormatPath(i.Path)
	if p == "" {
		return i.Message
	}
	return p + ": " + i.Message
}

// Error is returned by Validate when the value does not match the schema.
type Error struct {
	// Issues lists every problem found, in a stable order.
	Issues []Issue `json:"issues"`
}

// Error joins the issues with "; ".
func (e *Error) Error() string {
	parts := make([]string, len(e.Issues))
	for i, is := range e.Issues {
		parts[i] = is.String()
	}
	return strings.Join(parts, "; ")
}

// FormatPath formats an [Issue] path the way a JavaScript accessor would
// read, such as "members[2].name". An empty path formats as "".
func FormatPath(path []any) string {
	var b strings.Builder
	for _, p := range path {
		switch p := p.(type) {
		case int:
			b.WriteByte('[')
			b.WriteString(strconv.Itoa(p))
			b.WriteByte(']')
		default:
			if b.Len() > 0 {
				b.WriteByte('.')
			}
			fmt.Fprint(&b, p)
		}
	}
	return b.String()
}

// addIssue appends an issue at path to iss.
func addIssue(iss *[]Issue, path []any, code Code, msg string) {
	*iss = append(*iss, Issue{Path: slices.Clone(path), Code: code, Message: msg})
}

// child returns path with elem appended, without sharing path's backing
// array.
func child(path []any, elem any) []any {
	return append(slices.Clip(path), elem)
}

// invalidType appends a CodeInvalidType issue for v.
func invalidType(iss *[]Issue, path []any, want string, v any) {
	addIssue(iss, path, CodeInvalidType, "expected "+want+", got "+typeName(v))
}

// typeName names v's type the way JSON would.
func typeName(v any) string {
	if v == nil {
		return "null"
	}
	switch reflect.TypeOf(v).Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return "number"
	case reflect.String:
		if _, ok := v.(json.Number); ok {
			return "number"
		}
		return "string"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Map, reflect.Struct:
		return "object"
	}
	return fmt.Sprintf("%T", v)
}

// message returns custom[0] if given, else def.
func message(def string, custom []string) string {
	if len(custom) > 0 && custom[0] != "" {
		return custom[0]
	}
	return def
}

// check is one constraint on a value of type T.
type check[T any] struct {
	ok   func(T) bool
	code Code
	msg  string
	// kw and arg are the JSON Schema keyword and value that express the
	// check, such as "minLength" and 3. See [JSONSchema].
	kw  string
	arg any
}

// runChecks appends an issue for every check v fails, then runs the refine
// functions if no check failed.
func runChecks[T any](v T, checks []check[T], refines []func(T) error, path []any, iss *[]Issue) {
	failed := false
	for _, c := range checks {
		if !c.ok(v) {
			addIssue(iss, path, c.code, c.msg)
			failed = true
		}
	}
	if failed {
		return
	}
	for _, r := range refines {
		if err := r(v); err != nil {
			addIssue(iss, path, CodeCustom, err.Error())
			return
		}
	}
}

// copyValue deep-copies the maps and slices in a default value, so callers
// that modify Validate's output do not change the default.
func copyValue(v any) any {
	switch v := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			out[k] = copyValue(e)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = copyValue(e)
		}
		return out
	}
	return v
}

// plural returns "n word" with an s added to word when n is not 1.
func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return strconv.Itoa(n) + " " + word + "s"
}
