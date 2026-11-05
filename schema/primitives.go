package schema

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// StringSchema validates strings. Create one with [String].
type StringSchema struct {
	mods       modifiers
	transforms []func(string) string
	checks     []check[string]
	refines    []func(string) error
}

// String returns a schema that accepts strings. Lengths are counted in
// Unicode code points, not bytes.
func String() *StringSchema { return &StringSchema{} }

func (s *StringSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *StringSchema) modifiers() modifiers        { return s.mods }
func (s *StringSchema) Description() string         { return s.mods.description }

func (s *StringSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *StringSchema) parse(v any, path []any, iss *[]Issue) any {
	str, ok := asString(v)
	if !ok {
		invalidType(iss, path, "string", v)
		return nil
	}
	for _, t := range s.transforms {
		str = t(str)
	}
	runChecks(str, s.checks, s.refines, path, iss)
	return str
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *StringSchema) Optional() *StringSchema { return s.withModifiers(setOptional).(*StringSchema) }

// Nullable accepts null and returns it unchanged.
func (s *StringSchema) Nullable() *StringSchema { return s.withModifiers(setNullable).(*StringSchema) }

// Default fills in v when an object field is missing. v is not validated.
func (s *StringSchema) Default(v string) *StringSchema {
	return s.withModifiers(setDefault(v)).(*StringSchema)
}

// Describe sets text that documents this schema. [StringSchema.Description]
// returns it. The text does not affect validation.
func (s *StringSchema) Describe(text string) *StringSchema {
	return s.withModifiers(setDescription(text)).(*StringSchema)
}

// Refine adds a custom check. It runs after the built-in checks pass, and a
// non-nil error is reported with CodeCustom and the error's text.
func (s *StringSchema) Refine(fn func(string) error) *StringSchema {
	c := *s
	c.refines = append(slices.Clip(s.refines), fn)
	return &c
}

func (s *StringSchema) transform(fn func(string) string) *StringSchema {
	c := *s
	c.transforms = append(slices.Clip(s.transforms), fn)
	return &c
}

func (s *StringSchema) check(ok func(string) bool, code Code, msg, kw string, arg any) *StringSchema {
	c := *s
	c.checks = append(slices.Clip(s.checks), check[string]{ok: ok, code: code, msg: msg, kw: kw, arg: arg})
	return &c
}

// Trim removes leading and trailing white space. Like the other transforms,
// it runs before any check, wherever it appears in the chain.
func (s *StringSchema) Trim() *StringSchema { return s.transform(strings.TrimSpace) }

// ToLower converts the string to lower case before any check runs.
func (s *StringSchema) ToLower() *StringSchema { return s.transform(strings.ToLower) }

// ToUpper converts the string to upper case before any check runs.
func (s *StringSchema) ToUpper() *StringSchema { return s.transform(strings.ToUpper) }

// Min requires at least n characters. msg, if given, replaces the default
// message, here and in every other check.
func (s *StringSchema) Min(n int, msg ...string) *StringSchema {
	return s.check(func(v string) bool { return utf8.RuneCountInString(v) >= n },
		CodeTooSmall, message("must be at least "+plural(n, "character"), msg), "minLength", n)
}

// Max requires at most n characters.
func (s *StringSchema) Max(n int, msg ...string) *StringSchema {
	return s.check(func(v string) bool { return utf8.RuneCountInString(v) <= n },
		CodeTooBig, message("must be at most "+plural(n, "character"), msg), "maxLength", n)
}

// Length requires exactly n characters.
func (s *StringSchema) Length(n int, msg ...string) *StringSchema {
	m := message("must be exactly "+plural(n, "character"), msg)
	return s.check(func(v string) bool { return utf8.RuneCountInString(v) >= n }, CodeTooSmall, m, "minLength", n).
		check(func(v string) bool { return utf8.RuneCountInString(v) <= n }, CodeTooBig, m, "maxLength", n)
}

// NonEmpty rejects the empty string. Combine it with Trim to also reject
// strings of only white space.
func (s *StringSchema) NonEmpty(msg ...string) *StringSchema {
	return s.check(func(v string) bool { return v != "" }, CodeTooSmall, message("must not be empty", msg), "minLength", 1)
}

// Regex requires the string to match re.
func (s *StringSchema) Regex(re *regexp.Regexp, msg ...string) *StringSchema {
	return s.check(re.MatchString, CodeInvalidString, message("must match "+re.String(), msg), "pattern", re.String())
}

// StartsWith requires the string to start with prefix.
func (s *StringSchema) StartsWith(prefix string, msg ...string) *StringSchema {
	return s.check(func(v string) bool { return strings.HasPrefix(v, prefix) },
		CodeInvalidString, message("must start with "+strconv.Quote(prefix), msg),
		"pattern", "^"+regexp.QuoteMeta(prefix))
}

// EndsWith requires the string to end with suffix.
func (s *StringSchema) EndsWith(suffix string, msg ...string) *StringSchema {
	return s.check(func(v string) bool { return strings.HasSuffix(v, suffix) },
		CodeInvalidString, message("must end with "+strconv.Quote(suffix), msg),
		"pattern", regexp.QuoteMeta(suffix)+"$")
}

// Includes requires the string to contain substr.
func (s *StringSchema) Includes(substr string, msg ...string) *StringSchema {
	return s.check(func(v string) bool { return strings.Contains(v, substr) },
		CodeInvalidString, message("must include "+strconv.Quote(substr), msg),
		"pattern", regexp.QuoteMeta(substr))
}

var (
	emailRE = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)
	uuidRE  = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// Email requires something shaped like an email address: no white space,
// one @, and a dot in the domain. It does not check that the address exists.
func (s *StringSchema) Email(msg ...string) *StringSchema {
	return s.check(emailRE.MatchString, CodeInvalidString, message("must be a valid email address", msg), "format", "email")
}

// URL requires an absolute URL with a scheme and host, such as
// "https://example.com/a". Any scheme is accepted; add StartsWith or Refine
// to limit it.
func (s *StringSchema) URL(msg ...string) *StringSchema {
	return s.check(func(v string) bool {
		u, err := url.Parse(v)
		return err == nil && u.Scheme != "" && u.Host != ""
	}, CodeInvalidString, message("must be a valid URL", msg), "format", "uri")
}

// UUID requires a UUID in its canonical hyphenated form, in either case.
func (s *StringSchema) UUID(msg ...string) *StringSchema {
	return s.check(uuidRE.MatchString, CodeInvalidString, message("must be a valid UUID", msg), "format", "uuid")
}

// Datetime requires an RFC 3339 timestamp, such as
// "2026-10-09T14:30:00Z". The output is still the string.
func (s *StringSchema) Datetime(msg ...string) *StringSchema {
	return s.check(func(v string) bool {
		_, err := time.Parse(time.RFC3339Nano, v)
		return err == nil
	}, CodeInvalidString, message("must be an RFC 3339 date-time", msg), "format", "date-time")
}

// asString returns v as a string if v's kind is string. json.Number is a
// number, not a string.
func asString(v any) (string, bool) {
	switch v := v.(type) {
	case string:
		return v, true
	case json.Number, nil:
		return "", false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.String {
		return rv.String(), true
	}
	return "", false
}

// NumberSchema validates numbers and returns them as float64. Create one
// with [Number].
type NumberSchema struct {
	mods    modifiers
	checks  []check[float64]
	refines []func(float64) error
}

// Number returns a schema that accepts any finite number and returns it as a
// float64. NaN and infinities are rejected because JSON cannot encode them.
// Use [Int] for whole numbers.
func Number() *NumberSchema { return &NumberSchema{} }

func (s *NumberSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *NumberSchema) modifiers() modifiers        { return s.mods }
func (s *NumberSchema) Description() string         { return s.mods.description }

func (s *NumberSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *NumberSchema) parse(v any, path []any, iss *[]Issue) any {
	f, ok := asFloat(v)
	if !ok {
		invalidType(iss, path, "number", v)
		return nil
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		addIssue(iss, path, CodeInvalidType, "must be a finite number")
		return nil
	}
	runChecks(f, s.checks, s.refines, path, iss)
	return f
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *NumberSchema) Optional() *NumberSchema { return s.withModifiers(setOptional).(*NumberSchema) }

// Nullable accepts null and returns it unchanged.
func (s *NumberSchema) Nullable() *NumberSchema { return s.withModifiers(setNullable).(*NumberSchema) }

// Default fills in v when an object field is missing. v is not validated.
func (s *NumberSchema) Default(v float64) *NumberSchema {
	return s.withModifiers(setDefault(v)).(*NumberSchema)
}

// Describe sets text that documents this schema. [NumberSchema.Description]
// returns it. The text does not affect validation.
func (s *NumberSchema) Describe(text string) *NumberSchema {
	return s.withModifiers(setDescription(text)).(*NumberSchema)
}

// Refine adds a custom check. It runs after the built-in checks pass, and a
// non-nil error is reported with CodeCustom and the error's text.
func (s *NumberSchema) Refine(fn func(float64) error) *NumberSchema {
	c := *s
	c.refines = append(slices.Clip(s.refines), fn)
	return &c
}

func (s *NumberSchema) check(ok func(float64) bool, code Code, msg, kw string, arg any) *NumberSchema {
	c := *s
	c.checks = append(slices.Clip(s.checks), check[float64]{ok: ok, code: code, msg: msg, kw: kw, arg: arg})
	return &c
}

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

// Min requires the number to be at least n.
func (s *NumberSchema) Min(n float64, msg ...string) *NumberSchema {
	return s.check(func(v float64) bool { return v >= n }, CodeTooSmall, message("must be at least "+formatFloat(n), msg), "minimum", n)
}

// Max requires the number to be at most n.
func (s *NumberSchema) Max(n float64, msg ...string) *NumberSchema {
	return s.check(func(v float64) bool { return v <= n }, CodeTooBig, message("must be at most "+formatFloat(n), msg), "maximum", n)
}

// Gt requires the number to be greater than n.
func (s *NumberSchema) Gt(n float64, msg ...string) *NumberSchema {
	return s.check(func(v float64) bool { return v > n }, CodeTooSmall, message("must be greater than "+formatFloat(n), msg), "exclusiveMinimum", n)
}

// Lt requires the number to be less than n.
func (s *NumberSchema) Lt(n float64, msg ...string) *NumberSchema {
	return s.check(func(v float64) bool { return v < n }, CodeTooBig, message("must be less than "+formatFloat(n), msg), "exclusiveMaximum", n)
}

// Positive requires the number to be greater than 0.
func (s *NumberSchema) Positive(msg ...string) *NumberSchema {
	return s.Gt(0, message("must be positive", msg))
}

// NonNegative requires the number to be at least 0.
func (s *NumberSchema) NonNegative(msg ...string) *NumberSchema {
	return s.Min(0, message("must not be negative", msg))
}

// Negative requires the number to be less than 0.
func (s *NumberSchema) Negative(msg ...string) *NumberSchema {
	return s.Lt(0, message("must be negative", msg))
}

// NonPositive requires the number to be at most 0.
func (s *NumberSchema) NonPositive(msg ...string) *NumberSchema {
	return s.Max(0, message("must not be positive", msg))
}

// MultipleOf requires the number to be a whole multiple of step, allowing
// for floating-point rounding, so 0.3 is a multiple of 0.1.
func (s *NumberSchema) MultipleOf(step float64, msg ...string) *NumberSchema {
	return s.check(func(v float64) bool {
		q := v / step
		return math.Abs(q-math.Round(q)) < 1e-9
	}, CodeNotMultipleOf, message("must be a multiple of "+formatFloat(step), msg), "multipleOf", step)
}

// asFloat returns v as a float64 if it is a Go number or a json.Number.
func asFloat(v any) (float64, bool) {
	switch v := v.(type) {
	case float64:
		return v, true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case nil:
		return 0, false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	}
	return 0, false
}

// IntSchema validates whole numbers and returns them as int64. Create one
// with [Int].
type IntSchema struct {
	mods    modifiers
	checks  []check[int64]
	refines []func(int64) error
}

// Int returns a schema that accepts whole numbers that fit in an int64, and
// returns them as int64. A float64 such as 3.0, which is how JSON decodes 3,
// is accepted; 3.5 is not. Integers above 2^53 do not survive a trip
// through a JavaScript client, so add Min and Max when that matters.
func Int() *IntSchema { return &IntSchema{} }

func (s *IntSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *IntSchema) modifiers() modifiers        { return s.mods }
func (s *IntSchema) Description() string         { return s.mods.description }

func (s *IntSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *IntSchema) parse(v any, path []any, iss *[]Issue) any {
	n, isNum, isInt := asInt(v)
	if !isNum {
		invalidType(iss, path, "integer", v)
		return nil
	}
	if !isInt {
		addIssue(iss, path, CodeNotInteger, "must be an integer")
		return nil
	}
	runChecks(n, s.checks, s.refines, path, iss)
	return n
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *IntSchema) Optional() *IntSchema { return s.withModifiers(setOptional).(*IntSchema) }

// Nullable accepts null and returns it unchanged.
func (s *IntSchema) Nullable() *IntSchema { return s.withModifiers(setNullable).(*IntSchema) }

// Default fills in v when an object field is missing. v is not validated.
func (s *IntSchema) Default(v int64) *IntSchema { return s.withModifiers(setDefault(v)).(*IntSchema) }

// Describe sets text that documents this schema. [IntSchema.Description]
// returns it. The text does not affect validation.
func (s *IntSchema) Describe(text string) *IntSchema {
	return s.withModifiers(setDescription(text)).(*IntSchema)
}

// Refine adds a custom check. It runs after the built-in checks pass, and a
// non-nil error is reported with CodeCustom and the error's text.
func (s *IntSchema) Refine(fn func(int64) error) *IntSchema {
	c := *s
	c.refines = append(slices.Clip(s.refines), fn)
	return &c
}

func (s *IntSchema) check(ok func(int64) bool, code Code, msg, kw string, arg any) *IntSchema {
	c := *s
	c.checks = append(slices.Clip(s.checks), check[int64]{ok: ok, code: code, msg: msg, kw: kw, arg: arg})
	return &c
}

// Min requires the number to be at least n.
func (s *IntSchema) Min(n int64, msg ...string) *IntSchema {
	return s.check(func(v int64) bool { return v >= n }, CodeTooSmall, message(fmt.Sprintf("must be at least %d", n), msg), "minimum", n)
}

// Max requires the number to be at most n.
func (s *IntSchema) Max(n int64, msg ...string) *IntSchema {
	return s.check(func(v int64) bool { return v <= n }, CodeTooBig, message(fmt.Sprintf("must be at most %d", n), msg), "maximum", n)
}

// Positive requires the number to be greater than 0.
func (s *IntSchema) Positive(msg ...string) *IntSchema {
	return s.check(func(v int64) bool { return v > 0 }, CodeTooSmall, message("must be positive", msg), "exclusiveMinimum", 0)
}

// NonNegative requires the number to be at least 0.
func (s *IntSchema) NonNegative(msg ...string) *IntSchema {
	return s.check(func(v int64) bool { return v >= 0 }, CodeTooSmall, message("must not be negative", msg), "minimum", 0)
}

// Negative requires the number to be less than 0.
func (s *IntSchema) Negative(msg ...string) *IntSchema {
	return s.check(func(v int64) bool { return v < 0 }, CodeTooBig, message("must be negative", msg), "exclusiveMaximum", 0)
}

// NonPositive requires the number to be at most 0.
func (s *IntSchema) NonPositive(msg ...string) *IntSchema {
	return s.check(func(v int64) bool { return v <= 0 }, CodeTooBig, message("must not be positive", msg), "maximum", 0)
}

// MultipleOf requires the number to be a multiple of step. step must not be
// 0.
func (s *IntSchema) MultipleOf(step int64, msg ...string) *IntSchema {
	if step == 0 {
		panic("schema: Int().MultipleOf(0)")
	}
	return s.check(func(v int64) bool { return v%step == 0 }, CodeNotMultipleOf,
		message(fmt.Sprintf("must be a multiple of %d", step), msg), "multipleOf", step)
}

// asInt returns v as an int64. isNum reports whether v is a number at all,
// and isInt whether it is whole and fits in an int64.
func asInt(v any) (n int64, isNum, isInt bool) {
	if jn, ok := v.(json.Number); ok {
		if i, err := jn.Int64(); err == nil {
			return i, true, true
		}
	}
	if v != nil {
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int(), true, true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			u := rv.Uint()
			return int64(u), true, u <= math.MaxInt64
		}
	}
	f, ok := asFloat(v)
	if !ok {
		return 0, false, false
	}
	// -2^63 is exactly representable; 2^63 is the first float64 above
	// MaxInt64.
	if f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
		return 0, true, false
	}
	return int64(f), true, true
}

// BoolSchema validates booleans. Create one with [Bool].
type BoolSchema struct {
	mods modifiers
}

// Bool returns a schema that accepts true and false.
func Bool() *BoolSchema { return &BoolSchema{} }

func (s *BoolSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *BoolSchema) modifiers() modifiers        { return s.mods }
func (s *BoolSchema) Description() string         { return s.mods.description }

func (s *BoolSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *BoolSchema) parse(v any, path []any, iss *[]Issue) any {
	if v != nil && reflect.TypeOf(v).Kind() == reflect.Bool {
		return reflect.ValueOf(v).Bool()
	}
	invalidType(iss, path, "boolean", v)
	return nil
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *BoolSchema) Optional() *BoolSchema { return s.withModifiers(setOptional).(*BoolSchema) }

// Nullable accepts null and returns it unchanged.
func (s *BoolSchema) Nullable() *BoolSchema { return s.withModifiers(setNullable).(*BoolSchema) }

// Default fills in v when an object field is missing.
func (s *BoolSchema) Default(v bool) *BoolSchema { return s.withModifiers(setDefault(v)).(*BoolSchema) }

// Describe sets text that documents this schema. [BoolSchema.Description]
// returns it. The text does not affect validation.
func (s *BoolSchema) Describe(text string) *BoolSchema {
	return s.withModifiers(setDescription(text)).(*BoolSchema)
}

// LiteralSchema accepts one value. Create one with [Literal].
type LiteralSchema struct {
	mods  modifiers
	value any
}

// Literal returns a schema that accepts only value, which must be a string,
// bool or Go number. Numbers compare by value, so Literal(1) accepts the
// float64 1 that JSON decodes. The output is value in its JSON form.
// Literal panics if value has any other type.
func Literal(value any) *LiteralSchema {
	if s, ok := asString(value); ok {
		return &LiteralSchema{value: s}
	}
	if f, ok := asFloat(value); ok {
		return &LiteralSchema{value: f}
	}
	if b, ok := value.(bool); ok {
		return &LiteralSchema{value: b}
	}
	panic(fmt.Sprintf("schema: Literal does not support %T", value))
}

// Value returns the accepted value in its JSON form.
func (s *LiteralSchema) Value() any { return s.value }

func (s *LiteralSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *LiteralSchema) modifiers() modifiers        { return s.mods }
func (s *LiteralSchema) Description() string         { return s.mods.description }

func (s *LiteralSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *LiteralSchema) parse(v any, path []any, iss *[]Issue) any {
	var got any
	switch s.value.(type) {
	case string:
		got, _ = asString(v)
	case float64:
		got, _ = asFloat(v)
	case bool:
		if v != nil && reflect.TypeOf(v).Kind() == reflect.Bool {
			got = reflect.ValueOf(v).Bool()
		}
	}
	if got != s.value {
		addIssue(iss, path, CodeInvalidLiteral, "must be "+formatLiteral(s.value))
		return nil
	}
	return s.value
}

func formatLiteral(v any) string {
	switch v := v.(type) {
	case string:
		return strconv.Quote(v)
	case float64:
		return formatFloat(v)
	}
	return fmt.Sprint(v)
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *LiteralSchema) Optional() *LiteralSchema {
	return s.withModifiers(setOptional).(*LiteralSchema)
}

// Nullable accepts null and returns it unchanged.
func (s *LiteralSchema) Nullable() *LiteralSchema {
	return s.withModifiers(setNullable).(*LiteralSchema)
}

// Default fills in the literal's value when an object field is missing.
func (s *LiteralSchema) Default() *LiteralSchema {
	return s.withModifiers(setDefault(s.value)).(*LiteralSchema)
}

// Describe sets text that documents this schema. [LiteralSchema.Description]
// returns it. The text does not affect validation.
func (s *LiteralSchema) Describe(text string) *LiteralSchema {
	return s.withModifiers(setDescription(text)).(*LiteralSchema)
}

// EnumSchema accepts one of a fixed set of strings. Create one with [Enum].
type EnumSchema struct {
	mods   modifiers
	values []string
}

// Enum returns a schema that accepts only the given strings. It panics if
// no values are given.
func Enum(values ...string) *EnumSchema {
	if len(values) == 0 {
		panic("schema: Enum needs at least one value")
	}
	return &EnumSchema{values: slices.Clone(values)}
}

// Values returns a copy of the accepted strings, in the order given to
// [Enum].
func (s *EnumSchema) Values() []string { return slices.Clone(s.values) }

func (s *EnumSchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *EnumSchema) modifiers() modifiers        { return s.mods }
func (s *EnumSchema) Description() string         { return s.mods.description }

func (s *EnumSchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *EnumSchema) parse(v any, path []any, iss *[]Issue) any {
	str, ok := asString(v)
	if !ok {
		invalidType(iss, path, "string", v)
		return nil
	}
	if !slices.Contains(s.values, str) {
		quoted := make([]string, len(s.values))
		for i, e := range s.values {
			quoted[i] = strconv.Quote(e)
		}
		addIssue(iss, path, CodeInvalidEnum, "must be one of "+strings.Join(quoted, ", "))
		return nil
	}
	return str
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *EnumSchema) Optional() *EnumSchema { return s.withModifiers(setOptional).(*EnumSchema) }

// Nullable accepts null and returns it unchanged.
func (s *EnumSchema) Nullable() *EnumSchema { return s.withModifiers(setNullable).(*EnumSchema) }

// Default fills in v when an object field is missing. v is not validated.
func (s *EnumSchema) Default(v string) *EnumSchema {
	return s.withModifiers(setDefault(v)).(*EnumSchema)
}

// Describe sets text that documents this schema. [EnumSchema.Description]
// returns it. The text does not affect validation.
func (s *EnumSchema) Describe(text string) *EnumSchema {
	return s.withModifiers(setDescription(text)).(*EnumSchema)
}

// AnySchema accepts every value. Create one with [Any].
type AnySchema struct {
	mods    modifiers
	refines []func(any) error
}

// Any returns a schema that accepts every value, including null, and returns
// it unchanged. Add Refine to check it yourself.
func Any() *AnySchema { return &AnySchema{} }

func (s *AnySchema) Validate(v any) (any, error) { return validate(s, v) }
func (s *AnySchema) modifiers() modifiers        { return s.mods }
func (s *AnySchema) Description() string         { return s.mods.description }

func (s *AnySchema) withModifiers(f func(*modifiers)) Schema {
	c := *s
	f(&c.mods)
	return &c
}

func (s *AnySchema) parse(v any, path []any, iss *[]Issue) any {
	runChecks(v, nil, s.refines, path, iss)
	return v
}

// Optional lets an object field be missing. The field is then left out of
// the output.
func (s *AnySchema) Optional() *AnySchema { return s.withModifiers(setOptional).(*AnySchema) }

// Default fills in v when an object field is missing. Maps and slices in v
// are copied for each use.
func (s *AnySchema) Default(v any) *AnySchema { return s.withModifiers(setDefault(v)).(*AnySchema) }

// Describe sets text that documents this schema. [AnySchema.Description]
// returns it. The text does not affect validation.
func (s *AnySchema) Describe(text string) *AnySchema {
	return s.withModifiers(setDescription(text)).(*AnySchema)
}

// Refine adds a custom check. A non-nil error is reported with CodeCustom
// and the error's text. Refine also sees null.
func (s *AnySchema) Refine(fn func(any) error) *AnySchema {
	c := *s
	c.refines = append(slices.Clip(s.refines), fn)
	return &c
}
