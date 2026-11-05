package schema

import (
	"encoding/json"
	"regexp"
	"testing"
)

// wantJSONSchema checks that s exports as want, ignoring "$schema" and key
// order.
func wantJSONSchema(t *testing.T, s Schema, want string) {
	t.Helper()
	got := JSONSchema(s)
	if got["$schema"] != JSONSchemaDialect {
		t.Fatalf("$schema = %v", got["$schema"])
	}
	delete(got, "$schema")
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var w any
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("bad want: %v", err)
	}
	wantJSON, _ := json.Marshal(w)
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("JSONSchema =\n%s\nwant\n%s", gotJSON, wantJSON)
	}
}

func TestJSONSchemaString(t *testing.T) {
	wantJSONSchema(t, String(), `{"type":"string"}`)
	wantJSONSchema(t, String().Trim().Min(1).Max(10), `{"type":"string","minLength":1,"maxLength":10}`)
	wantJSONSchema(t, String().Length(3), `{"type":"string","minLength":3,"maxLength":3}`)
	wantJSONSchema(t, String().NonEmpty(), `{"type":"string","minLength":1}`)
	wantJSONSchema(t, String().Email(), `{"type":"string","format":"email"}`)
	wantJSONSchema(t, String().URL(), `{"type":"string","format":"uri"}`)
	wantJSONSchema(t, String().UUID(), `{"type":"string","format":"uuid"}`)
	wantJSONSchema(t, String().Datetime(), `{"type":"string","format":"date-time"}`)
	wantJSONSchema(t, String().Regex(regexp.MustCompile(`^[a-z]+$`)), `{"type":"string","pattern":"^[a-z]+$"}`)

	// Several patterns all apply.
	wantJSONSchema(t, String().StartsWith("a.").EndsWith("?").Includes("x"),
		`{"type":"string","pattern":"^a\\.","allOf":[{"pattern":"\\?$"},{"pattern":"x"}]}`)
	// So do repeated bounds.
	wantJSONSchema(t, String().Min(2).Min(5), `{"type":"string","minLength":2,"allOf":[{"minLength":5}]}`)
}

func TestJSONSchemaNumbers(t *testing.T) {
	wantJSONSchema(t, Number().Min(1.5).Max(9), `{"type":"number","minimum":1.5,"maximum":9}`)
	wantJSONSchema(t, Number().Gt(0).Lt(1), `{"type":"number","exclusiveMinimum":0,"exclusiveMaximum":1}`)
	wantJSONSchema(t, Number().Positive(), `{"type":"number","exclusiveMinimum":0}`)
	wantJSONSchema(t, Number().MultipleOf(0.5), `{"type":"number","multipleOf":0.5}`)

	wantJSONSchema(t, Int().Min(1).Max(100), `{"type":"integer","minimum":1,"maximum":100}`)
	wantJSONSchema(t, Int().Positive(), `{"type":"integer","exclusiveMinimum":0}`)
	wantJSONSchema(t, Int().NonNegative(), `{"type":"integer","minimum":0}`)
	wantJSONSchema(t, Int().Negative(), `{"type":"integer","exclusiveMaximum":0}`)
	wantJSONSchema(t, Int().NonPositive(), `{"type":"integer","maximum":0}`)
	wantJSONSchema(t, Int().MultipleOf(5), `{"type":"integer","multipleOf":5}`)
}

func TestJSONSchemaScalars(t *testing.T) {
	wantJSONSchema(t, Bool(), `{"type":"boolean"}`)
	wantJSONSchema(t, Literal("a"), `{"const":"a"}`)
	wantJSONSchema(t, Literal(2), `{"const":2}`)
	wantJSONSchema(t, Enum("a", "b"), `{"type":"string","enum":["a","b"]}`)
	wantJSONSchema(t, Any(), `{}`)
}

func TestJSONSchemaModifiers(t *testing.T) {
	wantJSONSchema(t, String().Nullable(), `{"type":["string","null"]}`)
	wantJSONSchema(t, Enum("a").Nullable(), `{"type":["string","null"],"enum":["a",null]}`)
	wantJSONSchema(t, Literal(true).Nullable(), `{"enum":[true,null]}`)
	wantJSONSchema(t, Union(String(), Int()).Nullable(),
		`{"anyOf":[{"type":"string"},{"type":"integer"},{"type":"null"}]}`)

	wantJSONSchema(t, Int().Default(3).Describe("count"), `{"type":"integer","default":3,"description":"count"}`)
	wantJSONSchema(t, Array(String()).Default([]any{"x"}), `{"type":"array","items":{"type":"string"},"default":["x"]}`)
}

func TestJSONSchemaComposite(t *testing.T) {
	wantJSONSchema(t, Array(Int()).NonEmpty().Max(3), `{"type":"array","items":{"type":"integer"},"minItems":1,"maxItems":3}`)
	wantJSONSchema(t, Array(Int()).Length(2), `{"type":"array","items":{"type":"integer"},"minItems":2,"maxItems":2}`)
	wantJSONSchema(t, Record(Bool()), `{"type":"object","additionalProperties":{"type":"boolean"}}`)
	wantJSONSchema(t, Union(String(), Literal(1)), `{"anyOf":[{"type":"string"},{"const":1}]}`)

	obj := Object(Shape{
		"name":  String(),
		"note":  String().Optional(),
		"count": Int().Default(0),
		"tags":  Array(Enum("x")),
	})
	wantJSONSchema(t, obj, `{
		"type": "object",
		"properties": {
			"count": {"type":"integer","default":0},
			"name":  {"type":"string"},
			"note":  {"type":"string"},
			"tags":  {"type":"array","items":{"type":"string","enum":["x"]}}
		},
		"required": ["name","tags"]
	}`)
	wantJSONSchema(t, Object(Shape{"a": Bool()}).Strict(),
		`{"type":"object","properties":{"a":{"type":"boolean"}},"required":["a"],"additionalProperties":false}`)
	wantJSONSchema(t, Object(Shape{"a": Bool()}).Partial(), `{"type":"object","properties":{"a":{"type":"boolean"}}}`)
	wantJSONSchema(t, Object(nil), `{"type":"object","properties":{}}`)

	// Nested schemas carry their own description, default and "$schema" is
	// only at the root.
	nested := Object(Shape{"inner": Object(Shape{"x": Int()}).Describe("inner")})
	wantJSONSchema(t, nested, `{
		"type": "object",
		"properties": {
			"inner": {"type":"object","properties":{"x":{"type":"integer"}},"required":["x"],"description":"inner"}
		},
		"required": ["inner"]
	}`)
}

func TestJSONSchemaFreshMap(t *testing.T) {
	def := []any{"x"}
	s := Array(String()).Default(def)
	out := JSONSchema(s)
	out["default"].([]any)[0] = "changed"
	out["type"] = "changed"
	if def[0] != "x" {
		t.Fatal("modifying the export changed the default")
	}
	if JSONSchema(s)["type"] != "array" {
		t.Fatal("modifying the export changed a later export")
	}
}
