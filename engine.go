package tether

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/recodeorg/tether/reactivity"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Engine struct {
	db              *gorm.DB
	dbType          string // sqlite or postgres
	mutations       map[string]Mutation
	queries         map[string]Query
	dependencies    map[string][]string
	hashMu          sync.RWMutex
	queryHashes     map[string]uint64
	tracker         *reactivity.Tracker
	auth            Auth
	websocketHelper *reactivity.WebsocketHelper
}

type Mutation struct {
	Func     func(ctx *MutationCtx) interface{}
	Internal bool
}

type Query struct {
	Func     func(ctx *QueryCtx) interface{}
	Internal bool
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
		mutations:       make(map[string]Mutation),
		queries:         make(map[string]Query),
		dependencies:    make(map[string][]string),
		queryHashes:     make(map[string]uint64),
		tracker:         tracker,
		auth:            defaultAuth{},
		websocketHelper: &reactivity.WebsocketHelper{},
	}
	invalidate := func(tx *gorm.DB) {
		if dbType == "postgres" {
			return
		}
		e.InvalidateTags(extractMutationTags(tx))
	}
	// GORM callbacks only see the post-update Dest, so a Save that moves a
	// tracked collection field would miss the old collection. Snapshot the
	// matching rows before the UPDATE SQL runs.
	snapshotOld := func(tx *gorm.DB) {
		if dbType == "postgres" {
			return
		}
		snapshotOldTrackedTags(tx)
	}
	db.Callback().Create().After("gorm:create").Register("tether:after_create", invalidate)
	db.Callback().Update().Before("gorm:update").Register("tether:before_update", snapshotOld)
	db.Callback().Update().After("gorm:update").Register("tether:after_update", invalidate)
	db.Callback().Delete().After("gorm:delete").Register("tether:after_delete", invalidate)

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

	seen := make(map[string]struct{})
	add := func(tag string) {
		if _, ok := seen[tag]; ok {
			return
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}

	// Emit table mutated tag
	add(fmt.Sprintf("table_%s:mutated", tableName))

	for _, item := range items {
		appendRecordTags(tx, item, add)
	}

	// Updates(map) stores assignments in Dest, not the row. Pull PK / collection
	// values from the map, the Model (e.g. Model(&msg).Updates(map)), and WHERE.
	if val.Kind() == reflect.Map {
		appendMapTags(tx, val, add)
	}
	if tx.Statement.Model != nil && tx.Statement.Model != tx.Statement.Dest {
		if modelVal := reflect.Indirect(reflect.ValueOf(tx.Statement.Model)); modelVal.Kind() == reflect.Struct {
			appendRecordTags(tx, modelVal, add)
		}
	}
	appendWhereTags(tx, add)

	if old, ok := tx.InstanceGet(oldTrackedTagsKey); ok {
		if oldTags, ok := old.([]string); ok {
			for _, tag := range oldTags {
				add(tag)
			}
		}
	}

	return tags
}

const oldTrackedTagsKey = "tether:old_tracked_tags"

func hasTrackedFields(tx *gorm.DB) bool {
	if tx.Statement == nil || tx.Statement.Schema == nil {
		return false
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") == "track" {
			return true
		}
	}
	return false
}

// snapshotOldTrackedTags loads the rows about to be updated and stashes their
// tracked-field tags on the statement. GORM does not expose a before-image in
// update callbacks, so this extra SELECT (same transaction, hooks skipped) is
// what lets a collection move invalidate both the old and new collections.
func snapshotOldTrackedTags(tx *gorm.DB) {
	if tx.Error != nil || tx.DryRun || tx.Statement == nil || tx.Statement.Schema == nil {
		return
	}
	if !hasTrackedFields(tx) {
		return
	}

	dest := tx.Statement.Schema.MakeSlice().Interface()
	q := tx.Session(&gorm.Session{
		NewDB:       true,
		SkipHooks:   true,
		Initialized: true,
		Context:     context.Background(),
	}).Model(reflect.New(tx.Statement.Schema.ModelType).Interface())
	if tx.Statement.Table != "" {
		q = q.Table(tx.Statement.Table)
	}
	q.Statement.Unscoped = tx.Statement.Unscoped

	if !restrictToUpdatingRows(tx, q) {
		return
	}
	if err := q.Find(dest).Error; err != nil {
		slog.Debug("tether: failed to snapshot rows before update", "error", err)
		return
	}

	seen := make(map[string]struct{})
	var tags []string
	add := func(tag string) {
		if _, ok := seen[tag]; ok {
			return
		}
		seen[tag] = struct{}{}
		tags = append(tags, tag)
	}
	slice := reflect.Indirect(reflect.ValueOf(dest))
	for i := 0; i < slice.Len(); i++ {
		appendRecordTags(tx, reflect.Indirect(slice.Index(i)), add)
	}
	if len(tags) > 0 {
		tx.InstanceSet(oldTrackedTagsKey, tags)
	}
}

// restrictToUpdatingRows copies the update's WHERE and/or primary keys onto q.
// Save() does not attach the PK to WHERE until gorm:update itself, so we also
// read PKs from Dest and Model. Returns false if we cannot identify rows
// without scanning the whole table.
func restrictToUpdatingRows(updateTx, queryTx *gorm.DB) bool {
	identified := false
	if where, ok := updateTx.Statement.Clauses["WHERE"]; ok && where.Expression != nil {
		queryTx.Statement.AddClause(clause.Where{Exprs: []clause.Expression{where.Expression}})
		identified = true
	}
	if addPrimaryKeyIdentity(updateTx, queryTx) {
		identified = true
	}
	return identified
}

func addPrimaryKeyIdentity(updateTx, queryTx *gorm.DB) bool {
	if updateTx.Statement.Schema == nil {
		return false
	}
	pks := updateTx.Statement.Schema.PrimaryFields
	if len(pks) == 0 {
		return false
	}

	var items []reflect.Value
	collectStructItems := func(v interface{}) {
		if v == nil {
			return
		}
		val := reflect.Indirect(reflect.ValueOf(v))
		switch val.Kind() {
		case reflect.Struct:
			items = append(items, val)
		case reflect.Slice:
			for i := 0; i < val.Len(); i++ {
				items = append(items, reflect.Indirect(val.Index(i)))
			}
		}
	}
	collectStructItems(updateTx.Statement.Dest)
	if updateTx.Statement.Model != nil && updateTx.Statement.Model != updateTx.Statement.Dest {
		collectStructItems(updateTx.Statement.Model)
	}

	var groups []clause.Expression
	for _, item := range items {
		if item.Kind() != reflect.Struct {
			continue
		}
		var eqs []clause.Expression
		complete := true
		for _, field := range pks {
			pkVal, isZero := field.ValueOf(updateTx.Statement.Context, item)
			if isZero {
				complete = false
				break
			}
			eqs = append(eqs, clause.Eq{Column: field.DBName, Value: pkVal})
		}
		if !complete || len(eqs) == 0 {
			continue
		}
		if len(eqs) == 1 {
			groups = append(groups, eqs[0])
		} else {
			groups = append(groups, clause.And(eqs...))
		}
	}
	if len(groups) == 0 {
		return false
	}
	if len(groups) == 1 {
		queryTx.Statement.AddClause(clause.Where{Exprs: groups})
	} else {
		queryTx.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.Or(groups...)}})
	}
	return true
}

func appendRecordTags(tx *gorm.DB, item reflect.Value, add func(string)) {
	tableName := tx.Statement.Table
	for _, field := range tx.Statement.Schema.PrimaryFields {
		if pkVal, isZero := field.ValueOf(tx.Statement.Context, item); !isZero {
			add(fmt.Sprintf("%s:%v", tableName, pkVal))
		}
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") == "track" {
			if trackVal, isZero := field.ValueOf(tx.Statement.Context, item); !isZero {
				// We use field.DBName to ensure it matches what the developer
				// writes in ctx.TrackCollection!
				add(fmt.Sprintf("%s_%s:%v", tableName, field.DBName, trackVal))
			}
		}
	}
}

func appendMapTags(tx *gorm.DB, val reflect.Value, add func(string)) {
	m, ok := mapStringInterface(val)
	if !ok {
		return
	}
	tableName := tx.Statement.Table
	for _, field := range tx.Statement.Schema.PrimaryFields {
		if v, exists := lookupMap(m, field.Name, field.DBName); exists && !isZeroValue(v) {
			add(fmt.Sprintf("%s:%v", tableName, v))
		}
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") != "track" {
			continue
		}
		if v, exists := lookupMap(m, field.Name, field.DBName); exists && !isZeroValue(v) {
			add(fmt.Sprintf("%s_%s:%v", tableName, field.DBName, v))
		}
	}
}

func appendWhereTags(tx *gorm.DB, add func(string)) {
	where, ok := tx.Statement.Clauses["WHERE"]
	if !ok || where.Expression == nil {
		return
	}
	walkWhereExpr(tx, where.Expression, add)
}

func walkWhereExpr(tx *gorm.DB, expr clause.Expression, add func(string)) {
	switch e := expr.(type) {
	case clause.Where:
		for _, x := range e.Exprs {
			walkWhereExpr(tx, x, add)
		}
	case clause.AndConditions:
		for _, x := range e.Exprs {
			walkWhereExpr(tx, x, add)
		}
	case clause.Eq:
		appendColumnValueTag(tx, e.Column, e.Value, add)
	case clause.IN:
		for _, v := range flattenIN(e.Values) {
			appendColumnValueTag(tx, e.Column, v, add)
		}
	case clause.Expr:
		appendSQLExprTags(tx, e, add)
	}
}

func appendColumnValueTag(tx *gorm.DB, column, value interface{}, add func(string)) {
	if isZeroValue(value) {
		return
	}
	tableName := tx.Statement.Table
	name := clauseColumnName(column)
	if name == clause.PrimaryKey {
		add(fmt.Sprintf("%s:%v", tableName, value))
		return
	}
	for _, field := range tx.Statement.Schema.PrimaryFields {
		if columnMatchesField(name, field.Name, field.DBName) {
			add(fmt.Sprintf("%s:%v", tableName, value))
			return
		}
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") != "track" {
			continue
		}
		if columnMatchesField(name, field.Name, field.DBName) {
			add(fmt.Sprintf("%s_%s:%v", tableName, field.DBName, value))
			return
		}
	}
}

func appendSQLExprTags(tx *gorm.DB, expr clause.Expr, add func(string)) {
	if len(expr.Vars) == 0 {
		return
	}
	sql := normalizeSQL(expr.SQL)
	for _, field := range tx.Statement.Schema.PrimaryFields {
		if sqlIsEquality(sql, field.Name, field.DBName) {
			appendColumnValueTag(tx, field.DBName, expr.Vars[0], add)
			return
		}
		if sqlIsIN(sql, field.Name, field.DBName) {
			for _, v := range flattenIN(expr.Vars) {
				appendColumnValueTag(tx, field.DBName, v, add)
			}
			return
		}
	}
	for _, field := range tx.Statement.Schema.Fields {
		if field.Tag.Get("tether") != "track" {
			continue
		}
		if sqlIsEquality(sql, field.Name, field.DBName) {
			appendColumnValueTag(tx, field.DBName, expr.Vars[0], add)
			return
		}
	}
}

func mapStringInterface(val reflect.Value) (map[string]interface{}, bool) {
	if val.Kind() != reflect.Map || val.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	if m, ok := val.Interface().(map[string]interface{}); ok {
		return m, true
	}
	m := make(map[string]interface{}, val.Len())
	iter := val.MapRange()
	for iter.Next() {
		m[iter.Key().String()] = iter.Value().Interface()
	}
	return m, true
}

func lookupMap(m map[string]interface{}, names ...string) (interface{}, bool) {
	for _, name := range names {
		if v, ok := m[name]; ok {
			return v, true
		}
	}
	return nil, false
}

func isZeroValue(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return !rv.IsValid() || rv.IsZero()
}

func clauseColumnName(col interface{}) string {
	switch c := col.(type) {
	case string:
		return c
	case clause.Column:
		return c.Name
	default:
		return fmt.Sprint(c)
	}
}

func columnMatchesField(col, fieldName, dbName string) bool {
	col = stripColumn(col)
	return strings.EqualFold(col, dbName) || strings.EqualFold(col, fieldName)
}

func stripColumn(col string) string {
	col = strings.Trim(col, "`\"[]")
	if i := strings.LastIndex(col, "."); i >= 0 {
		col = strings.Trim(col[i+1:], "`\"[]")
	}
	return col
}

func normalizeSQL(sql string) string {
	sql = strings.ToLower(sql)
	for _, q := range []string{"`", `"`, "[", "]"} {
		sql = strings.ReplaceAll(sql, q, "")
	}
	return strings.Join(strings.Fields(sql), " ")
}

func sqlIsEquality(sql, fieldName, dbName string) bool {
	for _, col := range sqlColumnNames(fieldName, dbName) {
		if sql == col+" = ?" || sql == col+"=?" ||
			strings.HasSuffix(sql, "."+col+" = ?") || strings.HasSuffix(sql, "."+col+"=?") {
			return true
		}
	}
	return false
}

func sqlIsIN(sql, fieldName, dbName string) bool {
	for _, col := range sqlColumnNames(fieldName, dbName) {
		if sql == col+" in ?" || sql == col+" in (?)" ||
			strings.HasSuffix(sql, "."+col+" in ?") || strings.HasSuffix(sql, "."+col+" in (?)") {
			return true
		}
	}
	return false
}

func sqlColumnNames(fieldName, dbName string) []string {
	names := []string{strings.ToLower(dbName), strings.ToLower(fieldName)}
	var out []string
	seen := make(map[string]struct{})
	for _, name := range names {
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func flattenIN(values []interface{}) []interface{} {
	var out []interface{}
	for _, v := range values {
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice && rv.Type() != reflect.TypeOf([]byte(nil)) {
			for i := 0; i < rv.Len(); i++ {
				out = append(out, rv.Index(i).Interface())
			}
			continue
		}
		out = append(out, v)
	}
	return out
}

func (e *Engine) SetAuth(auth Auth) {
	e.auth = auth
}

func (e *Engine) RegisterMutation(name string, mutation func(ctx *MutationCtx) interface{}, opts ...MutationOptions) {
	options := MutationOptions{
		Internal: false,
	}
	if len(opts) > 0 {
		options = opts[0]
	}
	e.mutations[name] = Mutation{Func: mutation, Internal: options.Internal} // stores the mutation in the list of valid mutations
	slog.Debug("Registered mutation", "name", name)
}

func (e *Engine) RegisterQuery(name string, query func(ctx *QueryCtx) interface{}, dependencies []string, opts ...QueryOptions) {
	options := QueryOptions{
		Internal: false,
	}
	if len(opts) > 0 {
		options = opts[0]
	}
	e.queries[name] = Query{Func: query, Internal: options.Internal} // stores the query in the list of valid queries
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
	e.InvalidateTags([]string{tag})
}

// InvalidateTags re-runs each distinct subscription that listens to any of the
// given tags. Subscriptions are snapshotted before any query executes so a
// re-run that picks up additional tags (e.g. auto-tracked primary keys) cannot
// cause the same mutation to fire the query a second time.
func (e *Engine) InvalidateTags(tags []string) {
	seen := make(map[string]struct{})
	var unique []*reactivity.Subscription
	for _, tag := range tags {
		for _, subscription := range e.tracker.GetSubscriptionsToTag(tag) {
			if _, ok := seen[subscription.SubID]; ok {
				continue
			}
			seen[subscription.SubID] = struct{}{}
			unique = append(unique, subscription)
		}
	}
	for _, subscription := range unique {
		e.ExecuteQuery(subscription.Query, subscription.Params, subscription, true)
	}
}

func (e *Engine) ExecuteQuery(query string, params map[string]interface{}, subscription *reactivity.Subscription, forceSend bool) (interface{}, error) {
	if _, exists := e.queries[query]; !exists {
		return nil, fmt.Errorf("query not found")
	}
	if e.queries[query].Internal {
		return nil, fmt.Errorf("query not found") // return non-descriptive error to prevent enumeration
	}
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
	result := e.queries[query].Func(queryCtx)

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

func (e *Engine) ExecuteMutationInternal(mutation string, params map[string]interface{}) (interface{}, error) {
	if _, exists := e.mutations[mutation]; !exists {
		return nil, fmt.Errorf("mutation not found")
	}
	authCtx := &AuthCtx{
		GetIdentity: func() (string, error) { panic("tether: mutations with auth cannot be executed internally") },
	}
	mutationCtx := &MutationCtx{DB: e.db, AuthCtx: authCtx, Params: params}
	result := e.mutations[mutation].Func(mutationCtx)
	slog.Debug("Executing mutation internally", "mutation", mutation, "params", params, "result", result)
	return result, nil
}

func (e *Engine) ExecuteMutation(mutation string, params map[string]interface{}, clientID string, mutationID string) (interface{}, error) {
	if _, exists := e.mutations[mutation]; !exists {
		return nil, fmt.Errorf("mutation not found")
	}
	if e.mutations[mutation].Internal {
		return nil, fmt.Errorf("mutation not found") // return non-descriptive error to prevent enumeration
	}
	auth, ok := e.tracker.GetAuth(clientID)
	if !ok {
		return nil, fmt.Errorf("client not found")
	}
	authID := auth.UserID

	authCtx := &AuthCtx{
		GetIdentity: func() (string, error) { return authID, nil },
	}

	mutationCtx := &MutationCtx{DB: e.db, AuthCtx: authCtx, Params: params}

	result := e.mutations[mutation].Func(mutationCtx)
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
		query, ok := msg["location"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		params, ok := msg["params"].(map[string]interface{})
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		queryKey, ok := msg["query_key"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		subscription := e.tracker.SubscribeToQuery(clientID, query, queryKey, params)
		if subscription == nil {
			slog.Error("Failed to subscribe to query", "query", query, "params", params)
			return nil
		}
		e.ExecuteQuery(query, params, subscription, true)
	case "mutation":
		mutation, ok := msg["location"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		params, ok := msg["params"].(map[string]interface{})
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		mutationID, ok := msg["mutation_id"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		e.ExecuteMutation(mutation, params, clientID, mutationID)
	case "auth":
		token, ok := msg["token"].(string)
		if !ok {
			slog.Error("Invalid message", "message", msg)
			return nil
		}
		userID, expiresAt, err := e.auth.VerifyToken(e.db, token)
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
