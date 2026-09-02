package tether

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/recodeorg/tether/reactivity"
	"gorm.io/gorm"
)

type Engine struct {
	db              *gorm.DB
	dbType          string // sqlite or postgres
	mutations       map[string]func(ctx *MutationCtx) interface{}
	queries         map[string]func(ctx *QueryCtx) interface{}
	dependencies    map[string][]string
	hashMu          sync.RWMutex
	queryHashes     map[string]uint64
	tracker         *reactivity.Tracker
	auth            Auth
	websocketHelper *reactivity.WebsocketHelper
}

type defaultAuth struct{}

type contextKey string

const tetherCtxKey contextKey = "tether_query_ctx"

func (defaultAuth) VerifyToken(_ *gorm.DB, _ string) (string, time.Time, error) {
	return "", time.Time{}, nil
}

func getIdentity(queryCtx *QueryCtx, authID string) (string, error) {
	if authID != "" {
		queryCtx.Dependencies = append(queryCtx.Dependencies, "*user_identity:"+authID)
	}
	return authID, nil
}

func NewEngine(db *gorm.DB, dbType string) *Engine {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	tracker := reactivity.NewTracker()
	if dbType != "sqlite" && dbType != "postgres" {
		panic("Invalid database type")
	}
	e := &Engine{
		db:              db,
		dbType:          dbType,
		mutations:       make(map[string]func(ctx *MutationCtx) interface{}),
		queries:         make(map[string]func(ctx *QueryCtx) interface{}),
		dependencies:    make(map[string][]string),
		queryHashes:     make(map[string]uint64),
		tracker:         tracker,
		auth:            defaultAuth{},
		websocketHelper: &reactivity.WebsocketHelper{},
	}
	db.Callback().Create().After("gorm:create").Register("tether:after_create", func(tx *gorm.DB) {
		if dbType == "postgres" {
			return
		}
		for _, tag := range extractMutationTags(tx) {
			e.InvalidateTag(tag)
		}
	})

	db.Callback().Update().After("gorm:update").Register("tether:after_update", func(tx *gorm.DB) {
		if dbType == "postgres" {
			return
		}
		for _, tag := range extractMutationTags(tx) {
			e.InvalidateTag(tag)
		}
	})

	db.Callback().Delete().After("gorm:delete").Register("tether:after_delete", func(tx *gorm.DB) {
		if dbType == "postgres" {
			return
		}
		for _, tag := range extractMutationTags(tx) {
			e.InvalidateTag(tag)
		}
	})

	// Automatically track the dependencies for the query
	db.Callback().Query().After("gorm:query").Register("tether:auto_track", func(tx *gorm.DB) {
		tCtx, ok := tx.Statement.Context.Value(tetherCtxKey).(*QueryCtx)
		if !ok || tx.Statement.Dest == nil || tx.Statement.Schema == nil {
			return
		}

		tableName := tx.Statement.Table
		val := reflect.Indirect(reflect.ValueOf(tx.Statement.Dest))

		var items []reflect.Value
		if val.Kind() == reflect.Slice {
			for i := 0; i < val.Len(); i++ {
				items = append(items, reflect.Indirect(val.Index(i)))
			}
		} else if val.Kind() == reflect.Struct {
			items = append(items, val)
		}

		for _, item := range items {
			for _, field := range tx.Statement.Schema.PrimaryFields {
				if pkVal, isZero := field.ValueOf(tx.Statement.Context, item); !isZero {
					tCtx.Dependencies = append(tCtx.Dependencies, fmt.Sprintf("%s:%v", tableName, pkVal))
				}
			}
		}
	})
	return e
}

// Helper function to extract the tags for a mutation
func extractMutationTags(tx *gorm.DB) []string {
	var tags []string
	// If GORM didn't parse a schema (e.g., raw SQL), we can't extract tags this way
	if tx.Statement.Schema == nil || tx.Statement.Dest == nil {
		return tags
	}

	tableName := tx.Statement.Table
	val := reflect.Indirect(reflect.ValueOf(tx.Statement.Dest))

	// Convert everything to a slice of reflect.Values so we can iterate uniformly
	var items []reflect.Value
	if val.Kind() == reflect.Slice {
		for i := 0; i < val.Len(); i++ {
			items = append(items, reflect.Indirect(val.Index(i)))
		}
	} else if val.Kind() == reflect.Struct {
		items = append(items, val)
	}

	for _, item := range items {
		// 1. Extract Primary Keys (e.g., "messages:5")
		for _, field := range tx.Statement.Schema.PrimaryFields {
			if pkVal, isZero := field.ValueOf(tx.Statement.Context, item); !isZero {
				tags = append(tags, fmt.Sprintf("%s:%v", tableName, pkVal))
			}
		}

		// 2. Extract explicit tether Collection tags (e.g., "messages_channel_id:5")
		for _, field := range tx.Statement.Schema.Fields {
			if field.Tag.Get("tether") == "track" {
				if trackVal, isZero := field.ValueOf(tx.Statement.Context, item); !isZero {
					// We use field.DBName to ensure it matches what the developer
					// writes in ctx.TrackCollection!
					tags = append(tags, fmt.Sprintf("%s_%s:%v", tableName, field.DBName, trackVal))
				}
			}
		}
	}

	return tags
}

func (e *Engine) SetAuth(auth Auth) {
	e.auth = auth
}

func (e *Engine) RegisterMutation(name string, mutation func(ctx *MutationCtx) interface{}) {
	e.mutations[name] = mutation // stores the mutation in the list of valid mutations
	slog.Debug("Registered mutation", "name", name)
}

func (e *Engine) RegisterQuery(name string, query func(ctx *QueryCtx) interface{}, dependencies []string) {
	e.queries[name] = query // stores the query in the list of valid queries
	for _, dependency := range dependencies {
		e.dependencies[dependency] = append(e.dependencies[dependency], name)
	}
	slog.Debug("Registered query", "name", name)
}

func (e *Engine) CreateTable(name string, schema interface{}) {
	e.db.AutoMigrate(schema)
	slog.Debug("Created table", "name", name)
}

func (e *Engine) Handle(w http.ResponseWriter, r *http.Request) {
	reactivity.Handle(w, r, e, e.tracker, e.websocketHelper) // wraps the raw websocket connection with the engine handler
}

func (e *Engine) SetCheckOrigin(checkOrigin func(r *http.Request) bool) {
	e.websocketHelper.CheckOrigin = checkOrigin
}

// Simple helper to set the allowed origins for the websocket connection
func (e *Engine) SetAllowedOrigins(allowedOrigins []string) {
	e.websocketHelper.CheckOrigin = func(r *http.Request) bool {
		return slices.Contains(allowedOrigins, r.Header.Get("Origin"))
	}
}

func (e *Engine) OnConnect(clientID string) error {
	slog.Debug("Connected to websocket", "client", clientID)
	// TODO: implement the logic to handle the connection
	return nil
}

func (e *Engine) OnDisconnect(clientID string) error {
	slog.Debug("Disconnected from websocket", "client", clientID)
	// TODO: implement the logic to handle the disconnection
	return nil
}

func (e *Engine) GetDependentQueries(tableName string) []string {
	return e.dependencies[tableName]
}

func (e *Engine) InvalidateTag(tag string) {
	subscriptions := e.tracker.GetSubscriptionsToTag(tag)
	for _, subscription := range subscriptions {
		e.ExecuteQuery(subscription.Query, subscription.Params, subscription, true)
	}
}

func (e *Engine) ExecuteQuery(query string, params map[string]interface{}, subscription *reactivity.Subscription, forceSend bool) (interface{}, error) {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	auth, ok := e.tracker.GetAuth(subscription.Client.ID)
	if !ok {
		return nil, fmt.Errorf("client not found")
	}
	authID := auth.UserID
	cacheKey := query + "?" + string(paramsJSON) + "?" + authID
	e.hashMu.Lock()
	lastHash := e.queryHashes[cacheKey]
	e.hashMu.Unlock()
	slog.Debug("Executing query", "query", query, "params", params)

	// Create the query context first so GetIdentity can append dependency tags.
	queryCtx := &QueryCtx{
		DB:           e.db,
		Params:       params,
		Dependencies: []string{},
	}
	queryCtx.Auth = &AuthCtx{
		GetIdentity: func() (string, error) {
			return getIdentity(queryCtx, authID)
		},
	}

	// Create the GORM context
	gormCtx := context.WithValue(context.Background(), tetherCtxKey, queryCtx)
	queryCtx.DB = e.db.WithContext(gormCtx)

	// Execute the query
	result := e.queries[query](queryCtx)

	// Update the dependencies
	e.tracker.UpdateTags(subscription.SubID, queryCtx.Dependencies)
	slog.Debug("Updated dependencies on subscription", "subID", subscription.SubID, "dependencies", queryCtx.Dependencies)

	// Serialize the result
	responseJSON, err := json.Marshal(map[string]interface{}{"type": "query", "location": query, "data": result, "query_key": subscription.QueryKey})
	if err != nil {
		return nil, err
	}
	queryHash := xxhash.Sum64(responseJSON)
	if lastHash == queryHash && !forceSend { // we want to force send on first subscription, regardless of if the query hasn't changed
		return result, nil
	}

	e.hashMu.Lock()
	e.queryHashes[cacheKey] = queryHash
	e.hashMu.Unlock()

	e.tracker.SendMessage(subscription.Client.ID, responseJSON)
	return result, nil
}

func (e *Engine) ExecuteMutation(mutation string, params map[string]interface{}, clientID string, mutationID string) (interface{}, error) {
	auth, ok := e.tracker.GetAuth(clientID)
	if !ok {
		return nil, fmt.Errorf("client not found")
	}
	authID := auth.UserID

	authCtx := &AuthCtx{
		GetIdentity: func() (string, error) { return authID, nil },
	}

	mutationCtx := &MutationCtx{DB: e.db, AuthCtx: authCtx, Params: params}

	result := e.mutations[mutation](mutationCtx)
	slog.Debug("Executing mutation", "mutation", mutation, "params", params, "result", result)
	responseJSON, err := json.Marshal(map[string]interface{}{"type": "mutation", "location": mutation, "data": result, "mutation_id": mutationID})
	if err != nil {
		slog.Error("Failed to encode mutation result", "mutation", mutation, "error", err)
		return nil, err
	}
	e.tracker.SendMessage(clientID, responseJSON)
	return result, nil
}

func (e *Engine) OnReceiveMessage(clientID string, msg map[string]interface{}) error {
	slog.Debug("Received message", "from", clientID, "message", msg)
	switch msg["type"] {
	case "subscribe":
		query := msg["location"].(string)
		params := msg["params"].(map[string]interface{})
		queryKey := msg["query_key"].(string)
		subscription := e.tracker.SubscribeToQuery(clientID, query, queryKey, params)
		if subscription == nil {
			slog.Error("Failed to subscribe to query", "query", query, "params", params)
			return nil
		}
		e.ExecuteQuery(query, params, subscription, true)
	case "mutation":
		e.ExecuteMutation(msg["location"].(string), msg["params"].(map[string]interface{}), clientID, msg["mutation_id"].(string))
	case "auth":
		userID, expiresAt, err := e.auth.VerifyToken(e.db, msg["token"].(string))
		time.AfterFunc(time.Until(expiresAt), func() {
			auth, ok := e.tracker.GetAuth(clientID)
			if !ok {
				return
			}
			if time.Time.Equal(auth.ExpiresAt, expiresAt) {
				e.tracker.SetAuth(clientID, "", time.Time{})
			}
		})
		if err != nil {
			slog.Error("Failed to get user ID", "error", err)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Failed to get user ID"}`))
			return err
		}
		e.tracker.SetAuth(clientID, userID, expiresAt)
		message := map[string]interface{}{"type": "auth", "success": true, "data": map[string]interface{}{"user_id": userID}}
		messageJSON, err := json.Marshal(message)
		if err != nil {
			slog.Error("Failed to encode auth message", "error", err)
			e.tracker.SendMessage(clientID, []byte(`{"type": "error", "error": "Failed to encode auth message"}`))
			return err
		}
		e.tracker.SendMessage(clientID, messageJSON)
		return nil
	}
	return nil
}
