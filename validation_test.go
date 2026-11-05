package tether

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/recodeorg/tether/schema"
)

func TestHandlerInputSchemas(t *testing.T) {
	for _, kind := range []string{"query", "live query", "mutation", "client mutation", "guard"} {
		t.Run(kind, func(t *testing.T) {
			e := newTestEngine(t)
			input := schema.Object(schema.Shape{
				"name":  schema.String().Trim().Min(1),
				"count": schema.Int().Default(2),
			})
			output := schema.Bool() // Intentionally differs from the result; never validated.
			calls := 0
			handler := func(params map[string]any) (any, error) {
				calls++
				return params, nil
			}
			var run func(map[string]any) (any, error)
			switch kind {
			case "query", "live query":
				e.RegisterQuery("test", func(ctx *QueryCtx) (any, error) { return handler(ctx.Params) }, WithInputSchema(input), WithOutputSchema(output))
				if e.queries["test"].InputSchema != input || e.queries["test"].OutputSchema != output {
					t.Fatal("query schemas not stored")
				}
			case "mutation", "client mutation":
				e.RegisterMutation("test", func(ctx *MutationCtx) (any, error) { return handler(ctx.Params) }, WithInputSchema(input), WithOutputSchema(output))
				if e.mutations["test"].InputSchema != input || e.mutations["test"].OutputSchema != output {
					t.Fatal("mutation schemas not stored")
				}
			case "guard":
				e.RegisterGuard("test", func(ctx *GuardCtx) (any, error) { return handler(ctx.Params) }, WithInputSchema(input), WithOutputSchema(output))
				if e.guards["test"].InputSchema != input || e.guards["test"].OutputSchema != output {
					t.Fatal("guard schemas not stored")
				}
			}
			switch kind {
			case "query":
				run = func(p map[string]any) (any, error) { return e.ExecuteQuery("test", p) }
			case "live query":
				client := trackClient(t, e)
				run = func(p map[string]any) (any, error) {
					sub := e.tracker.SubscribeToQuery(client.ID, "test", "key", p)
					auth, _ := e.tracker.GetAuth(client.ID)
					result, _, _, err := e.runQuery("test", p, sub, auth)
					if failure, ok := result.(error); ok {
						return nil, failure
					}
					return result, err
				}
			case "mutation":
				run = func(p map[string]any) (any, error) { return e.ExecuteMutation("test", p) }
			case "client mutation":
				client := trackClient(t, e)
				run = func(p map[string]any) (any, error) {
					result, err := e.executeMutation("test", p, client.ID, "m1")
					if failure, ok := result.(error); ok {
						frames := drain(client)
						if len(frames) != 1 || frames[0]["type"] != "error" || frames[0]["mutation_id"] != "m1" {
							t.Fatalf("validation error frames = %#v", frames)
						}
						return nil, failure
					}
					return result, err
				}
			case "guard":
				run = func(p map[string]any) (any, error) { return executeGuard(e, &GuardCtx{Params: p}, "test") }
			}
			for _, params := range []map[string]any{nil, {}, {"name": 42}, {"name": " "}} {
				_, err := run(params)
				var validationErr *schema.Error
				if !errors.As(err, &validationErr) {
					t.Fatalf("error = %v, want schema.Error", err)
				}
				if len(params) == 0 && (len(validationErr.Issues) != 1 || validationErr.Issues[0].Code != schema.CodeRequired || !reflect.DeepEqual(validationErr.Issues[0].Path, []any{"name"})) {
					t.Fatalf("missing field issues = %#v", validationErr.Issues)
				}
				if calls != 0 {
					t.Fatal("handler ran on invalid input")
				}
			}
			params := map[string]any{"name": " Ada ", "extra": true}
			got, err := run(params)
			want := map[string]any{"name": "Ada", "count": int64(2)}
			if err != nil || !reflect.DeepEqual(got, want) || calls != 1 {
				t.Fatalf("result = %#v, %v; calls = %d", got, err, calls)
			}
			if params["name"] != " Ada " || params["extra"] != true || len(params) != 2 {
				t.Fatalf("caller params changed: %#v", params)
			}
		})
	}
}

func TestHTTPActionSchemas(t *testing.T) {
	e := newTestEngine(t)
	input := schema.Object(schema.Shape{"name": schema.String().Trim(), "count": schema.Int().Default(3)})
	output := schema.Bool()
	calls := 0
	handler := e.HTTPAction(func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		calls++
		if want := (map[string]any{"name": "Ada", "count": int64(3)}); !reflect.DeepEqual(ctx.Params, want) {
			t.Errorf("params = %#v, want %#v", ctx.Params, want)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"name":" Ada "}` {
			t.Errorf("body = %q", body)
		}
		w.Write([]byte("output is not validated"))
	}, WithInputSchema(input), WithOutputSchema(output))
	if len(e.httpActions) != 1 || e.httpActions[0].InputSchema != input || e.httpActions[0].OutputSchema != output {
		t.Fatal("HTTP action schemas not stored")
	}
	for _, body := range []string{"", "{", "{}", `{"name":4}`, "null", "[]", `{"name":"Ada"} {}`} {
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
		if rec.Code != http.StatusBadRequest || calls != 0 {
			t.Fatalf("body %q: status = %d, calls = %d", body, rec.Code, calls)
		}
	}
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":" Ada "}`)))
	if rec.Code != http.StatusOK || calls != 1 || rec.Body.String() != "output is not validated" {
		t.Fatalf("status = %d, calls = %d, body = %q", rec.Code, calls, rec.Body.String())
	}
}

func TestSchemaOptionsOrderAndNoInputSchema(t *testing.T) {
	e := newTestEngine(t)
	params := map[string]any{"extra": true}
	e.RegisterQuery("plain", func(ctx *QueryCtx) (any, error) {
		ctx.Params["same map"] = true
		return ctx.Params, nil
	}, nil, Internal(), WithInputSchema(schema.Object(schema.Shape{"required": schema.String()})), WithInputSchema(nil), WithOutputSchema(schema.Bool()))
	if _, err := e.ExecuteQuery("plain", params); err != nil || params["same map"] != true {
		t.Fatalf("schema-free query changed behavior: params = %#v, error = %v", params, err)
	}
	if !e.queries["plain"].Internal {
		t.Fatal("schema options lost Internal")
	}
	called := false
	handler := e.HTTPAction(func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		called = true
		if ctx.Params != nil {
			t.Errorf("schema-free action params = %#v", ctx.Params)
		}
		body, _ := io.ReadAll(r.Body)
		w.Write(body)
	}, WithOutputSchema(schema.Bool()))
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader("raw webhook payload")))
	if !called || rec.Body.String() != "raw webhook payload" {
		t.Fatalf("schema-free action changed behavior: %q", rec.Body.String())
	}
}
