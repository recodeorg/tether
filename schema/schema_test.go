package schema

import (
	"encoding/json"
	"errors"
	"maps"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// issues validates v against s and returns the issues, or nil if it passed.
func issues(t *testing.T, s Schema, v any) []Issue {
	t.Helper()
	_, err := s.Validate(v)
	if err == nil {
		return nil
	}
	var se *Error
	if !errors.As(err, &se) {
		t.Fatalf("Validate returned %T, want *Error", err)
	}
	return se.Issues
}

func mustValidate(t *testing.T, s Schema, v any) any {
	t.Helper()
	out, err := s.Validate(v)
	if err != nil {
		t.Fatalf("Validate(%#v): %v", v, err)
	}
	return out
}

func wantCode(t *testing.T, s Schema, v any, code Code) {
	t.Helper()
	iss := issues(t, s, v)
	if len(iss) != 1 || iss[0].Code != code {
		t.Fatalf("Validate(%#v) issues = %v, want one %s", v, iss, code)
	}
}

func TestString(t *testing.T) {
	s := String().Min(2).Max(4)
	if got := mustValidate(t, s, "abc"); got != "abc" {
		t.Fatalf("got %v", got)
	}
	wantCode(t, s, "a", CodeTooSmall)
	wantCode(t, s, "abcde", CodeTooBig)
	wantCode(t, s, 5.0, CodeInvalidType)
	wantCode(t, s, nil, CodeInvalidType)
	wantCode(t, s, json.Number("5"), CodeInvalidType)

	// Lengths count code points.
	mustValidate(t, String().Length(2), "é😀")
	wantCode(t, String().Length(2), "abc", CodeTooBig)

	type role string
	if got := mustValidate(t, String(), role("admin")); got != "admin" {
		t.Fatalf("named string: got %#v", got)
	}
}

func TestStringTransforms(t *testing.T) {
	s := String().Min(1).Trim().ToLower()
	if got := mustValidate(t, s, "  HeLLo "); got != "hello" {
		t.Fatalf("got %q", got)
	}
	wantCode(t, s, "   ", CodeTooSmall)
}

func TestStringFormats(t *testing.T) {
	cases := []struct {
		s    *StringSchema
		good []string
		bad  []string
	}{
		{String().Email(), []string{"a@b.co", "first.last+tag@sub.example.org"}, []string{"a@b", "a b@c.de", "@b.co", "a@@b.co"}},
		{String().URL(), []string{"https://example.com", "http://localhost:8080/a?b=c"}, []string{"example.com", "/path", "https://"}},
		{String().UUID(), []string{"123e4567-e89b-12d3-a456-426614174000", "123E4567-E89B-12D3-A456-426614174000"}, []string{"123e4567e89b12d3a456426614174000", "nope"}},
		{String().Datetime(), []string{"2026-10-09T14:30:00Z", "2026-10-09T14:30:00.123+02:00"}, []string{"2026-10-09", "yesterday"}},
		{String().Regex(regexp.MustCompile(`^[a-z]+$`)), []string{"abc"}, []string{"ABC", ""}},
		{String().StartsWith("ab").EndsWith("yz").Includes("mm"), []string{"abmmyz"}, []string{"abyz", "mmyz"}},
		{String().NonEmpty(), []string{" "}, []string{""}},
	}
	for _, c := range cases {
		for _, g := range c.good {
			mustValidate(t, c.s, g)
		}
		for _, b := range c.bad {
			if len(issues(t, c.s, b)) == 0 {
				t.Errorf("%q passed, want an issue", b)
			}
		}
	}
}

func TestCustomMessage(t *testing.T) {
	iss := issues(t, String().Min(1, "name is required"), "")
	if len(iss) != 1 || iss[0].Message != "name is required" {
		t.Fatalf("issues = %v", iss)
	}
}

func TestReportsEveryFailedCheck(t *testing.T) {
	iss := issues(t, String().Min(5).Email(), "a")
	if len(iss) != 2 {
		t.Fatalf("issues = %v, want 2", iss)
	}
}

func TestRefine(t *testing.T) {
	calls := 0
	s := String().Min(3).Refine(func(v string) error {
		calls++
		if v == "root" {
			return errors.New("reserved name")
		}
		return nil
	})
	wantCode(t, s, "root", CodeCustom)
	wantCode(t, s, "ab", CodeTooSmall)
	if calls != 1 {
		t.Fatalf("refine ran %d times, want 1: it must not run after a failed check", calls)
	}
	if iss := issues(t, s, "root"); iss[0].Message != "reserved name" {
		t.Fatalf("message = %q", iss[0].Message)
	}
}

func TestNumber(t *testing.T) {
	s := Number().Min(0).Max(10)
	for _, v := range []any{5.0, 5, int8(5), uint64(5), float32(5), json.Number("5")} {
		if got := mustValidate(t, s, v); got != 5.0 {
			t.Fatalf("Validate(%T) = %#v, want float64 5", v, got)
		}
	}
	wantCode(t, s, -1.0, CodeTooSmall)
	wantCode(t, s, 11.0, CodeTooBig)
	wantCode(t, s, "5", CodeInvalidType)
	wantCode(t, s, math.NaN(), CodeInvalidType)
	wantCode(t, Number(), math.Inf(1), CodeInvalidType)

	wantCode(t, Number().Positive(), 0.0, CodeTooSmall)
	mustValidate(t, Number().NonNegative(), 0.0)
	wantCode(t, Number().Negative(), 0.0, CodeTooBig)
	wantCode(t, Number().Gt(1), 1.0, CodeTooSmall)
	wantCode(t, Number().Lt(1), 1.0, CodeTooBig)
	mustValidate(t, Number().MultipleOf(0.1), 0.3)
	wantCode(t, Number().MultipleOf(0.25), 0.3, CodeNotMultipleOf)
}

func TestInt(t *testing.T) {
	s := Int().Min(1)
	for _, v := range []any{3.0, 3, uint8(3), json.Number("3")} {
		if got := mustValidate(t, s, v); got != int64(3) {
			t.Fatalf("Validate(%T) = %#v, want int64 3", v, got)
		}
	}
	wantCode(t, s, 3.5, CodeNotInteger)
	wantCode(t, s, json.Number("3.5"), CodeNotInteger)
	wantCode(t, s, math.Pow(2, 63), CodeNotInteger)
	wantCode(t, s, uint64(math.MaxUint64), CodeNotInteger)
	wantCode(t, s, "3", CodeInvalidType)
	wantCode(t, s, 0.0, CodeTooSmall)
	wantCode(t, Int().MultipleOf(2), 3.0, CodeNotMultipleOf)
	if got := mustValidate(t, Int(), json.Number("9007199254740993")); got != int64(9007199254740993) {
		t.Fatalf("large json.Number lost precision: %v", got)
	}
}

func TestBoolLiteralEnum(t *testing.T) {
	if got := mustValidate(t, Bool(), true); got != true {
		t.Fatal(got)
	}
	wantCode(t, Bool(), "true", CodeInvalidType)

	if got := mustValidate(t, Literal(1), 1.0); got != 1.0 {
		t.Fatalf("Literal(1) = %#v", got)
	}
	mustValidate(t, Literal("a"), "a")
	mustValidate(t, Literal(false), false)
	wantCode(t, Literal("a"), "b", CodeInvalidLiteral)
	wantCode(t, Literal(true), "true", CodeInvalidLiteral)

	e := Enum("red", "green")
	mustValidate(t, e, "green")
	wantCode(t, e, "blue", CodeInvalidEnum)
	wantCode(t, e, 1.0, CodeInvalidType)
}

func TestAny(t *testing.T) {
	for _, v := range []any{nil, "x", 1.0, map[string]any{}} {
		if got := mustValidate(t, Any(), v); !reflect.DeepEqual(got, v) {
			t.Fatalf("Any changed %#v to %#v", v, got)
		}
	}
	wantCode(t, Any().Refine(func(v any) error {
		if v == nil {
			return errors.New("no nulls")
		}
		return nil
	}), nil, CodeCustom)
}

func TestNullable(t *testing.T) {
	if got := mustValidate(t, String().Nullable(), nil); got != nil {
		t.Fatal(got)
	}
	wantCode(t, String().Optional(), nil, CodeInvalidType)
}

func TestObject(t *testing.T) {
	s := Object(Shape{
		"name":  String().Min(1),
		"age":   Int().Optional(),
		"role":  Enum("admin", "member").Default("member"),
		"email": String().Email().Nullable(),
	})
	out := mustValidate(t, s, map[string]any{"name": "Ada", "email": nil, "extra": 1.0})
	want := map[string]any{"name": "Ada", "role": "member", "email": nil}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("got %#v, want %#v", out, want)
	}

	iss := issues(t, s, map[string]any{"name": "", "age": "x"})
	got := make([]string, len(iss))
	for i, is := range iss {
		got[i] = is.String() + " (" + string(is.Code) + ")"
	}
	wantIss := []string{
		"age: expected integer, got string (invalid_type)",
		"email: required (required)",
		"name: must be at least 1 character (too_small)",
	}
	if !reflect.DeepEqual(got, wantIss) {
		t.Fatalf("issues:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(wantIss, "\n"))
	}

	wantCode(t, s, []any{}, CodeInvalidType)
	wantCode(t, s, nil, CodeInvalidType)
}

func TestObjectNilMapIsEmpty(t *testing.T) {
	var params map[string]any
	out := mustValidate(t, Object(Shape{"page": Int().Default(1)}), params)
	if !reflect.DeepEqual(out, map[string]any{"page": int64(1)}) {
		t.Fatalf("got %#v", out)
	}
}

func TestObjectUnknownKeys(t *testing.T) {
	base := Object(Shape{"a": String()})
	in := map[string]any{"a": "x", "z": 1.0, "b": 2.0}

	if out := mustValidate(t, base, in); len(out.(map[string]any)) != 1 {
		t.Fatalf("strip: got %#v", out)
	}
	if out := mustValidate(t, base.Passthrough(), in); len(out.(map[string]any)) != 3 {
		t.Fatalf("passthrough: got %#v", out)
	}
	iss := issues(t, base.Strict(), in)
	if len(iss) != 1 || iss[0].Code != CodeUnrecognizedKeys || iss[0].Message != `unrecognized keys "b", "z"` {
		t.Fatalf("strict: %v", iss)
	}
	mustValidate(t, base.Strict().Strip(), in)
}

func TestObjectDerivation(t *testing.T) {
	user := Object(Shape{"name": String(), "email": String().Email(), "age": Int()})

	if keys := keysOf(user.Pick("name").Shape()); !reflect.DeepEqual(keys, []string{"name"}) {
		t.Fatalf("Pick: %v", keys)
	}
	if keys := keysOf(user.Omit("age").Shape()); !reflect.DeepEqual(keys, []string{"email", "name"}) {
		t.Fatalf("Omit: %v", keys)
	}
	ext := user.Extend(Shape{"age": String(), "bio": String()})
	mustValidate(t, ext, map[string]any{"name": "a", "email": "a@b.co", "age": "old", "bio": ""})

	partial := user.Partial()
	mustValidate(t, partial, map[string]any{})
	wantCode(t, partial, map[string]any{"email": "nope"}, CodeInvalidString)
	wantCode(t, partial.Required(), map[string]any{"name": "a", "email": "a@b.co"}, CodeRequired)

	// The original is unchanged.
	wantCode(t, user, map[string]any{"name": "a", "email": "a@b.co"}, CodeRequired)
}

func keysOf(s Shape) []string {
	return slices.Sorted(maps.Keys(s))
}

func TestObjectRefine(t *testing.T) {
	s := Object(Shape{"min": Number(), "max": Number()}).Refine(func(m map[string]any) error {
		if m["min"].(float64) > m["max"].(float64) {
			return errors.New("min must not exceed max")
		}
		return nil
	})
	mustValidate(t, s, map[string]any{"min": 1.0, "max": 2.0})
	wantCode(t, s, map[string]any{"min": 3.0, "max": 2.0}, CodeCustom)
	// The refine sees only valid fields, so it cannot panic on a bad type.
	wantCode(t, s, map[string]any{"min": "x", "max": 2.0}, CodeInvalidType)
}

func TestNestedPaths(t *testing.T) {
	s := Object(Shape{
		"members": Array(Object(Shape{"name": String().Min(1)})),
	})
	iss := issues(t, s, map[string]any{"members": []any{
		map[string]any{"name": "a"},
		map[string]any{"name": "b"},
		map[string]any{"name": ""},
	}})
	if len(iss) != 1 || !reflect.DeepEqual(iss[0].Path, []any{"members", 2, "name"}) {
		t.Fatalf("issues = %#v", iss)
	}
	if got := iss[0].String(); got != "members[2].name: must be at least 1 character" {
		t.Fatalf("String() = %q", got)
	}
}

func TestArray(t *testing.T) {
	s := Array(Int()).Min(1).Max(3)
	out := mustValidate(t, s, []any{1.0, 2.0})
	if !reflect.DeepEqual(out, []any{int64(1), int64(2)}) {
		t.Fatalf("got %#v", out)
	}
	if out := mustValidate(t, s, []int{4}); !reflect.DeepEqual(out, []any{int64(4)}) {
		t.Fatalf("[]int: got %#v", out)
	}
	wantCode(t, s, []any{}, CodeTooSmall)
	wantCode(t, s, []any{1.0, 2.0, 3.0, 4.0}, CodeTooBig)
	wantCode(t, Array(Int()).Length(2), []any{1.0}, CodeTooSmall)
	wantCode(t, s, map[string]any{}, CodeInvalidType)
	wantCode(t, Array(String()).NonEmpty(), []any{}, CodeTooSmall)

	iss := issues(t, Array(Int()), []any{1.0, "x", 2.5})
	if len(iss) != 2 || iss[0].Path[0] != 1 || iss[1].Path[0] != 2 {
		t.Fatalf("issues = %v", iss)
	}
}

func TestRecord(t *testing.T) {
	s := Record(Number())
	out := mustValidate(t, s, map[string]int{"a": 1})
	if !reflect.DeepEqual(out, map[string]any{"a": 1.0}) {
		t.Fatalf("got %#v", out)
	}
	iss := issues(t, s, map[string]any{"a": "x", "b": 1.0})
	if len(iss) != 1 || iss[0].Path[0] != "a" {
		t.Fatalf("issues = %v", iss)
	}
}

func TestUnion(t *testing.T) {
	s := Union(String(), Number())
	mustValidate(t, s, "a")
	mustValidate(t, s, 1.0)
	wantCode(t, s, true, CodeInvalidUnion)
	wantCode(t, s, nil, CodeInvalidUnion)
	if got := mustValidate(t, Union(String(), Number().Nullable()), nil); got != nil {
		t.Fatal(got)
	}

	// When only one option accepts the value's type, its issues are more
	// useful than a generic one.
	shape := Union(Object(Shape{"r": Number().Positive()}), String())
	iss := issues(t, shape, map[string]any{"r": -1.0})
	if len(iss) != 1 || iss[0].Code != CodeTooSmall || iss[0].Path[0] != "r" {
		t.Fatalf("issues = %v", iss)
	}

	wantCode(t, Union(Literal("a"), Literal("b")), "c", CodeInvalidUnion)
}

func TestDefaultIsCopied(t *testing.T) {
	s := Object(Shape{"tags": Array(String()).Default([]any{"x"})})
	out := mustValidate(t, s, map[string]any{}).(map[string]any)
	out["tags"].([]any)[0] = "changed"
	out = mustValidate(t, s, map[string]any{}).(map[string]any)
	if out["tags"].([]any)[0] != "x" {
		t.Fatal("modifying the output changed the default")
	}
}

func TestImmutable(t *testing.T) {
	base := String().Min(1)
	_ = base.Max(2)
	_ = base.Optional()
	mustValidate(t, base, "long string")
	obj := Object(Shape{"a": base})
	wantCode(t, obj, map[string]any{}, CodeRequired)

	// Appending to two schemas derived from one must not share checks.
	a := base.Max(3)
	b := base.Email()
	mustValidate(t, a, "abc")
	mustValidate(t, b, "longer@example.com")
}

func TestOutputDoesNotAliasInput(t *testing.T) {
	in := map[string]any{"a": []any{"x"}}
	out := mustValidate(t, Object(Shape{"a": Array(String())}), in).(map[string]any)
	out["a"].([]any)[0] = "y"
	out["b"] = 1
	if in["a"].([]any)[0] != "x" || len(in) != 1 {
		t.Fatal("output aliases input")
	}
}

func TestParse(t *testing.T) {
	type msg struct {
		Room  string   `json:"room"`
		Body  string   `json:"body"`
		Count int      `json:"count"`
		Tags  []string `json:"tags"`
	}
	s := Object(Shape{
		"room":  String(),
		"body":  String().Trim(),
		"count": Int().Default(1),
		"tags":  Array(String()).Default([]any{}),
	})
	got, err := Parse[msg](s, map[string]any{"room": "r", "body": " hi "})
	if err != nil {
		t.Fatal(err)
	}
	if want := (msg{Room: "r", Body: "hi", Count: 1, Tags: []string{}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}

	n, err := Parse[int64](Int(), 4.0)
	if err != nil || n != 4 {
		t.Fatalf("Parse[int64] = %v, %v", n, err)
	}

	if _, err := Parse[msg](s, map[string]any{}); err == nil {
		t.Fatal("want a validation error")
	} else if _, ok := err.(*Error); !ok {
		t.Fatalf("err = %T, want *Error", err)
	}
}

func TestDescription(t *testing.T) {
	if String().Description() != "" {
		t.Fatal("unset description should be empty")
	}
	base := String().Min(1)
	described := base.Describe("a name")
	if described.Description() != "a name" || base.Description() != "" {
		t.Fatal("Describe changed the original schema")
	}
	if described.Optional().Description() != "a name" {
		t.Fatal("Optional dropped the description")
	}
	replaced := described.Describe("other")
	if replaced.Description() != "other" || described.Description() != "a name" {
		t.Fatal("a second Describe changed the earlier schema")
	}
	mustValidate(t, described, "ok")
	wantCode(t, described, "", CodeTooSmall)

	field := String().Describe("id").Optional()
	obj := Object(Shape{"id": field}).Required()
	if obj.Shape()["id"].Description() != "id" {
		t.Fatal("Required dropped the field description")
	}

	schemas := []Schema{
		String().Describe("s"),
		Number().Describe("n"),
		Int().Describe("i"),
		Bool().Describe("b"),
		Literal("a").Describe("l"),
		Enum("a").Describe("e"),
		Any().Describe("y"),
		Array(String()).Describe("a"),
		Object(nil).Describe("o"),
		Record(String()).Describe("r"),
		Union(String(), Int()).Describe("u"),
	}
	for _, s := range schemas {
		if s.Description() == "" {
			t.Fatalf("%T description is empty", s)
		}
	}
}

func TestErrorJSON(t *testing.T) {
	_, err := Object(Shape{"a": Array(Int())}).Validate(map[string]any{"a": []any{"x"}})
	b, _ := json.Marshal(err)
	want := `{"issues":[{"path":["a",0],"code":"invalid_type","message":"expected integer, got string"}]}`
	if string(b) != want {
		t.Fatalf("got %s\nwant %s", b, want)
	}
	if err.Error() != "a[0]: expected integer, got string" {
		t.Fatalf("Error() = %q", err.Error())
	}
}
