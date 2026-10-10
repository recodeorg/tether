package schema

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// ArraySchema validates arrays. Create one with [Array].
type ArraySchema struct {
	mods    modifiers
	item    Schema
	checks  []check[int]
	refines []func([]any) error
}

// Array returns a schema that accepts arrays whose every element matches
// item, and returns them as []any. Every invalid element is reported, with
// its index in the issue's path.
func Array(item Schema) *ArraySchema { return &ArraySchema{item: item} }

// Item returns the schema for the array's elements.
func (s *ArraySchema) Item() Schema { return s.item }

func (s *ArraySchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *ArraySchema) modifiers() modifiers        { return s.mods }
func (s *ArraySchema) Description() string         { return s.mods.description }

func (s *ArraySchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *ArraySchema) parse(v any, path []any, iss *[]Issue) any {
	in, ok := asSlice(v)
	if !ok {
		invalidType(iss, path, "array", v)
		return nil
	}
	before := len(*iss)
	runChecks(len(in), s.checks, nil, path, iss)
	out := make([]any, len(in))
	for i, e := range in {
		out[i] = run(s.item, e, child(path, i), iss)
	}
	if len(*iss) == before {
		runChecks(out, nil, s.refines, path, iss)
	}
	return out
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *ArraySchema) Optional() *ArraySchema { return s.withModifiers(setOptional).(*ArraySchema) }

// Nullable accepts null and returns it unchanged.
func (s *ArraySchema) Nullable() *ArraySchema { return s.withModifiers(setNullable).(*ArraySchema) }

// Default fills in v when an object field is missing. v is not validated,
// and is copied for each use.
func (s *ArraySchema) Default(v []any) *ArraySchema {
	return s.withModifiers(setDefault(v)).(*ArraySchema)
}

// Describe sets text that documents this schema. [ArraySchema.Description]
// returns it. The text does not affect validation.
func (s *ArraySchema) Describe(text string) *ArraySchema {
	return s.withModifiers(setDescription(text)).(*ArraySchema)
}

// Refine adds a custom check on the validated elements. It runs only when
// the array and every element are valid, and a non-nil error is reported
// with CodeCustom and the error's text.
func (s *ArraySchema) Refine(fn func([]any) error) *ArraySchema {
	c := *s
	c.refines = append(slices.Clip(s.refines), fn)
	return &c
}

func (s *ArraySchema) check(ok func(int) bool, code Code, msg, kw string, arg any) *ArraySchema {
	c := *s
	c.checks = append(slices.Clip(s.checks), check[int]{ok: ok, code: code, msg: msg, kw: kw, arg: arg})
	return &c
}

// Min requires at least n elements.
func (s *ArraySchema) Min(n int, msg ...string) *ArraySchema {
	return s.check(func(l int) bool { return l >= n }, CodeTooSmall, message("must have at least "+plural(n, "item"), msg), "minItems", n)
}

// Max requires at most n elements.
func (s *ArraySchema) Max(n int, msg ...string) *ArraySchema {
	return s.check(func(l int) bool { return l <= n }, CodeTooBig, message("must have at most "+plural(n, "item"), msg), "maxItems", n)
}

// Length requires exactly n elements.
func (s *ArraySchema) Length(n int, msg ...string) *ArraySchema {
	m := message("must have exactly "+plural(n, "item"), msg)
	return s.check(func(l int) bool { return l >= n }, CodeTooSmall, m, "minItems", n).
		check(func(l int) bool { return l <= n }, CodeTooBig, m, "maxItems", n)
}

// NonEmpty requires at least one element.
func (s *ArraySchema) NonEmpty(msg ...string) *ArraySchema {
	return s.Min(1, message("must not be empty", msg))
}

// asSlice returns v's elements if v is a slice or array. A nil []any is an
// empty array, not null.
func asSlice(v any) ([]any, bool) {
	switch v := v.(type) {
	case []any:
		return v, true
	case nil:
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

// Shape maps an object's field names to their schemas.
type Shape map[string]Schema

// unknownKeys is how an object handles keys its shape does not list.
type unknownKeys int

const (
	stripUnknown unknownKeys = iota
	rejectUnknown
	keepUnknown
)

// ObjectSchema validates objects with known fields. Create one with
// [Object].
type ObjectSchema struct {
	mods    modifiers
	shape   Shape
	keys    []string // shape's keys, sorted
	unknown unknownKeys
	refines []func(map[string]any) error
}

// Object returns a schema that accepts objects whose fields match shape, and
// returns them as map[string]any. A field is required unless its schema is
// Optional or has a Default. Keys shape does not list are removed from the
// output; see [ObjectSchema.Strict] and [ObjectSchema.Passthrough].
//
// Fields are checked in sorted key order, so issues are listed in a stable
// order. A nil map, such as the params of a client that sent none, is an
// object with no fields.
func Object(shape Shape) *ObjectSchema {
	s := &ObjectSchema{shape: maps.Clone(shape)}
	if s.shape == nil {
		s.shape = Shape{}
	}
	s.keys = slices.Sorted(maps.Keys(s.shape))
	return s
}

// Shape returns a copy of the object's fields.
func (s *ObjectSchema) Shape() Shape { return maps.Clone(s.shape) }

func (s *ObjectSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *ObjectSchema) modifiers() modifiers        { return s.mods }
func (s *ObjectSchema) Description() string         { return s.mods.description }

func (s *ObjectSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *ObjectSchema) parse(v any, path []any, iss *[]Issue) any {
	in, ok := asMap(v)
	if !ok {
		invalidType(iss, path, "object", v)
		return nil
	}
	before := len(*iss)
	out := make(map[string]any, len(s.shape))
	for _, k := range s.keys {
		field := s.shape[k]
		e, present := in[k]
		if !present {
			val, omit, ok := missing(field)
			if !ok {
				addIssue(iss, child(path, k), CodeRequired, "required")
			} else if !omit {
				out[k] = val
			}
			continue
		}
		out[k] = run(field, e, child(path, k), iss)
	}
	if s.unknown != stripUnknown {
		var extra []string
		for k, e := range in {
			if _, known := s.shape[k]; !known {
				extra = append(extra, k)
				out[k] = e
			}
		}
		if s.unknown == rejectUnknown && len(extra) > 0 {
			slices.Sort(extra)
			quoted := make([]string, len(extra))
			for i, k := range extra {
				quoted[i] = strconv.Quote(k)
			}
			addIssue(iss, path, CodeUnrecognizedKeys, "unrecognized "+pluralWord(len(extra), "key")+" "+strings.Join(quoted, ", "))
		}
	}
	if len(*iss) == before {
		runChecks(out, nil, s.refines, path, iss)
	}
	return out
}

func pluralWord(n int, word string) string {
	if n == 1 {
		return word
	}
	return word + "s"
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *ObjectSchema) Optional() *ObjectSchema { return s.withModifiers(setOptional).(*ObjectSchema) }

// Nullable accepts null and returns it unchanged.
func (s *ObjectSchema) Nullable() *ObjectSchema { return s.withModifiers(setNullable).(*ObjectSchema) }

// Default fills in v when an object field is missing. v is not validated,
// and is copied for each use.
func (s *ObjectSchema) Default(v map[string]any) *ObjectSchema {
	return s.withModifiers(setDefault(v)).(*ObjectSchema)
}

// Describe sets text that documents this schema. [ObjectSchema.Description]
// returns it. The text does not affect validation.
func (s *ObjectSchema) Describe(text string) *ObjectSchema {
	return s.withModifiers(setDescription(text)).(*ObjectSchema)
}

// Refine adds a custom check on the validated object, such as one that
// compares two fields. It runs only when every field is valid, and a non-nil
// error is reported with CodeCustom and the error's text.
func (s *ObjectSchema) Refine(fn func(map[string]any) error) *ObjectSchema {
	c := *s
	c.refines = append(slices.Clip(s.refines), fn)
	return &c
}

// Strict rejects keys the shape does not list, with one CodeUnrecognizedKeys
// issue naming them.
func (s *ObjectSchema) Strict() *ObjectSchema {
	c := *s
	c.unknown = rejectUnknown
	return &c
}

// Passthrough keeps keys the shape does not list, unvalidated, in the
// output.
func (s *ObjectSchema) Passthrough() *ObjectSchema {
	c := *s
	c.unknown = keepUnknown
	return &c
}

// Strip removes keys the shape does not list from the output. This is the
// default; Strip undoes Strict or Passthrough.
func (s *ObjectSchema) Strip() *ObjectSchema {
	c := *s
	c.unknown = stripUnknown
	return &c
}

// withShape returns a copy of s with a new shape.
func (s *ObjectSchema) withShape(shape Shape) *ObjectSchema {
	c := *s
	c.shape = shape
	c.keys = slices.Sorted(maps.Keys(shape))
	return &c
}

// Extend adds fields, replacing any with the same name.
func (s *ObjectSchema) Extend(fields Shape) *ObjectSchema {
	shape := maps.Clone(s.shape)
	maps.Copy(shape, fields)
	return s.withShape(shape)
}

// Pick keeps only the named fields. It panics if a name is not in the shape.
func (s *ObjectSchema) Pick(keys ...string) *ObjectSchema {
	shape := make(Shape, len(keys))
	for _, k := range keys {
		f, ok := s.shape[k]
		if !ok {
			panic(fmt.Sprintf("schema: Pick: no field %q", k))
		}
		shape[k] = f
	}
	return s.withShape(shape)
}

// Omit removes the named fields. It panics if a name is not in the shape.
func (s *ObjectSchema) Omit(keys ...string) *ObjectSchema {
	shape := maps.Clone(s.shape)
	for _, k := range keys {
		if _, ok := shape[k]; !ok {
			panic(fmt.Sprintf("schema: Omit: no field %q", k))
		}
		delete(shape, k)
	}
	return s.withShape(shape)
}

// Partial makes every field optional, such as for a mutation that updates
// only the fields it is given. Nested objects are unchanged.
func (s *ObjectSchema) Partial() *ObjectSchema {
	shape := make(Shape, len(s.shape))
	for k, f := range s.shape {
		shape[k] = f.withModifiers(setOptional)
	}
	return s.withShape(shape)
}

// Required makes every field required, undoing Optional, Default and
// Partial. Nested objects are unchanged.
func (s *ObjectSchema) Required() *ObjectSchema {
	shape := make(Shape, len(s.shape))
	for k, f := range s.shape {
		shape[k] = f.withModifiers(func(m *modifiers) {
			m.optional = false
			m.hasDefault = false
			m.def = nil
		})
	}
	return s.withShape(shape)
}

// asMap returns v as a map[string]any if it is a map with string keys. The
// returned map must not be modified.
func asMap(v any) (map[string]any, bool) {
	switch v := v.(type) {
	case map[string]any:
		return v, true
	case nil:
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	out := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		out[iter.Key().String()] = iter.Value().Interface()
	}
	return out, true
}

// RecordSchema validates objects used as maps from any key to values of one
// type. Create one with [Record].
type RecordSchema struct {
	mods    modifiers
	value   Schema
	refines []func(map[string]any) error
}

// Record returns a schema that accepts objects whose every value matches
// value, whatever the keys, and returns them as map[string]any.
func Record(value Schema) *RecordSchema { return &RecordSchema{value: value} }

func (s *RecordSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *RecordSchema) modifiers() modifiers        { return s.mods }
func (s *RecordSchema) Description() string         { return s.mods.description }

func (s *RecordSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *RecordSchema) parse(v any, path []any, iss *[]Issue) any {
	in, ok := asMap(v)
	if !ok {
		invalidType(iss, path, "object", v)
		return nil
	}
	before := len(*iss)
	out := make(map[string]any, len(in))
	for _, k := range slices.Sorted(maps.Keys(in)) {
		out[k] = run(s.value, in[k], child(path, k), iss)
	}
	if len(*iss) == before {
		runChecks(out, nil, s.refines, path, iss)
	}
	return out
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *RecordSchema) Optional() *RecordSchema { return s.withModifiers(setOptional).(*RecordSchema) }

// Nullable accepts null and returns it unchanged.
func (s *RecordSchema) Nullable() *RecordSchema { return s.withModifiers(setNullable).(*RecordSchema) }

// Default fills in v when an object field is missing. v is not validated,
// and is copied for each use.
func (s *RecordSchema) Default(v map[string]any) *RecordSchema {
	return s.withModifiers(setDefault(v)).(*RecordSchema)
}

// Describe sets text that documents this schema. [RecordSchema.Description]
// returns it. The text does not affect validation.
func (s *RecordSchema) Describe(text string) *RecordSchema {
	return s.withModifiers(setDescription(text)).(*RecordSchema)
}

// Refine adds a custom check on the validated map. It runs only when every
// value is valid, and a non-nil error is reported with CodeCustom and the
// error's text.
func (s *RecordSchema) Refine(fn func(map[string]any) error) *RecordSchema {
	c := *s
	c.refines = append(slices.Clip(s.refines), fn)
	return &c
}

// UnionSchema accepts a value that matches any of several schemas. Create
// one with [Union].
type UnionSchema struct {
	mods    modifiers
	options []Schema
}

// Union returns a schema that accepts a value matching any of options, and
// returns the output of the first one that matches. It accepts null if any
// option is Nullable. When no option matches, the issues of the option that
// got furthest are reported if exactly one option failed for a reason other
// than the value's type; otherwise one CodeInvalidUnion issue is. Union
// panics if no options are given.
func Union(options ...Schema) *UnionSchema {
	if len(options) == 0 {
		panic("schema: Union needs at least one option")
	}
	return &UnionSchema{options: slices.Clone(options)}
}

// Options returns a copy of the union's schemas.
func (s *UnionSchema) Options() []Schema { return slices.Clone(s.options) }

func (s *UnionSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *UnionSchema) modifiers() modifiers        { return s.mods }
func (s *UnionSchema) Description() string         { return s.mods.description }

func (s *UnionSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *UnionSchema) parse(v any, path []any, iss *[]Issue) any {
	// close holds the issues of options that accepted v's type but
	// rejected its value, such as an object with one bad field.
	var close [][]Issue
	for _, o := range s.options {
		var oiss []Issue
		out := run(o, v, path, &oiss)
		if len(oiss) == 0 {
			return out
		}
		if !(len(oiss) == 1 && oiss[0].Code == CodeInvalidType && len(oiss[0].Path) == len(path)) {
			close = append(close, oiss)
		}
	}
	if len(close) == 1 {
		*iss = append(*iss, close[0]...)
		return nil
	}
	addIssue(iss, path, CodeInvalidUnion, "does not match any allowed type")
	return nil
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *UnionSchema) Optional() *UnionSchema { return s.withModifiers(setOptional).(*UnionSchema) }

// Nullable accepts null and returns it unchanged.
func (s *UnionSchema) Nullable() *UnionSchema { return s.withModifiers(setNullable).(*UnionSchema) }

// Default fills in v when an object field is missing. v is not validated,
// and maps and slices in it are copied for each use.
func (s *UnionSchema) Default(v any) *UnionSchema {
	return s.withModifiers(setDefault(v)).(*UnionSchema)
}

// Describe sets text that documents this schema. [UnionSchema.Description]
// returns it. The text does not affect validation.
func (s *UnionSchema) Describe(text string) *UnionSchema {
	return s.withModifiers(setDescription(text)).(*UnionSchema)
}
