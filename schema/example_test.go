package schema_test

import (
	"encoding/json"
	"fmt"

	"github.com/recodeorg/tether/schema"
)

func Example() {
	sendMessage := schema.Object(schema.Shape{
		"roomID": schema.String().UUID(),
		"body":   schema.String().Trim().Min(1).Max(2000),
		"tags":   schema.Array(schema.Enum("urgent", "fyi")).Max(5).Optional(),
		"pinned": schema.Bool().Default(false),
	})

	params := map[string]any{
		"roomID": "123e4567-e89b-12d3-a456-426614174000",
		"body":   "  hello  ",
	}
	out, err := sendMessage.Validate(params)
	fmt.Println(out, err)

	_, err = sendMessage.Validate(map[string]any{"body": "", "tags": []any{"urgent", "spam"}})
	fmt.Println(err)
	// Output:
	// map[body:hello pinned:false roomID:123e4567-e89b-12d3-a456-426614174000] <nil>
	// body: must be at least 1 character; roomID: required; tags[1]: must be one of "urgent", "fyi"
}

func ExampleJSONSchema() {
	search := schema.Object(schema.Shape{
		"query": schema.String().Min(1).Describe("text to search for"),
		"limit": schema.Int().Min(1).Max(50).Default(10),
	}).Strict()

	b, _ := json.MarshalIndent(schema.JSONSchema(search), "", "  ")
	fmt.Println(string(b))
	// Output:
	// {
	//   "$schema": "https://json-schema.org/draft/2020-12/schema",
	//   "additionalProperties": false,
	//   "properties": {
	//     "limit": {
	//       "default": 10,
	//       "maximum": 50,
	//       "minimum": 1,
	//       "type": "integer"
	//     },
	//     "query": {
	//       "description": "text to search for",
	//       "minLength": 1,
	//       "type": "string"
	//     }
	//   },
	//   "required": [
	//     "query"
	//   ],
	//   "type": "object"
	// }
}

func ExampleParse() {
	type Pagination struct {
		Page    int64 `json:"page"`
		PerPage int64 `json:"perPage"`
	}
	pagination := schema.Object(schema.Shape{
		"page":    schema.Int().Min(1).Default(1),
		"perPage": schema.Int().Min(1).Max(100).Default(20),
	})

	p, err := schema.Parse[Pagination](pagination, map[string]any{"page": 3.0})
	fmt.Printf("%+v %v\n", p, err)
	// Output:
	// {Page:3 PerPage:20} <nil>
}
