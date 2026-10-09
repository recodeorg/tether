package tether

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/recodeorg/tether/storage"
	"github.com/recodeorg/tether/storage/local"
	"gorm.io/gorm"
)

type headerAuth struct {
	userID string
	err    error
	tokens []string
	sawDB  bool
}

func (a *headerAuth) VerifyToken(ctx context.Context, db *gorm.DB, token string) (string, time.Time, error) {
	a.tokens = append(a.tokens, token)
	a.sawDB = db != nil
	if a.err != nil {
		return "", time.Time{}, a.err
	}
	return a.userID, time.Now().Add(time.Hour), nil
}

func callAction(e *Engine, headers map[string]string, fn func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request)) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/hooks/example", nil)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	e.HTTPAction(fn)(rec, req)
	return rec
}

func newStorageEngine(t *testing.T) (*Engine, *local.Storage) {
	t.Helper()
	e := newTestEngine(t)
	store, err := local.New(t.TempDir())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	if err := e.SetStorage(store, ""); err != nil {
		t.Fatalf("set storage: %v", err)
	}
	return e, store
}

func waitForParams(t *testing.T, got <-chan map[string]interface{}) map[string]interface{} {
	t.Helper()
	select {
	case params := <-got:
		return params
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for post-upload mutation")
		return nil
	}
}

func TestHTTPActionIdentity(t *testing.T) {
	e := newTestEngine(t)
	auth := &headerAuth{userID: "alice"}
	e.SetAuth(auth)

	var userID string
	var authErr error
	rec := callAction(e, map[string]string{"Authorization": "Bearer session-token"}, func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		userID, authErr = ctx.Auth.GetIdentity()
		if ctx.Profiler != e.Profiler() {
			t.Errorf("Profiler = %p, want the engine profiler", ctx.Profiler)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if authErr != nil || userID != "alice" {
		t.Fatalf("GetIdentity = %q, %v; want alice", userID, authErr)
	}
	if len(auth.tokens) != 1 || auth.tokens[0] != "session-token" || !auth.sawDB {
		t.Fatalf("VerifyToken tokens = %#v, sawDB = %v; want [session-token] with a database", auth.tokens, auth.sawDB)
	}

	rec = callAction(e, nil, func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		userID, authErr = ctx.Auth.GetIdentity()
	})
	if rec.Code != http.StatusOK || authErr != nil || userID != "" {
		t.Fatalf("anonymous GetIdentity = %q, %v, status %d; want empty identity", userID, authErr, rec.Code)
	}
	if len(auth.tokens) != 1 {
		t.Fatalf("anonymous request called VerifyToken, tokens = %#v", auth.tokens)
	}

	rec = callAction(e, map[string]string{"Authorization": "Token session-token"}, func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		userID, authErr = ctx.Auth.GetIdentity()
		w.WriteHeader(http.StatusAccepted)
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("handler status = %d, want 202; a rejected token still runs the handler", rec.Code)
	}
	if authErr == nil || authErr.Error() != "invalid authorization token" || userID != "" {
		t.Fatalf("GetIdentity = %q, %v; want invalid authorization token", userID, authErr)
	}
	if len(auth.tokens) != 1 {
		t.Fatalf("malformed header called VerifyToken, tokens = %#v", auth.tokens)
	}

	auth.err = errors.New("expired")
	callAction(e, map[string]string{"Authorization": "Bearer bad"}, func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		userID, authErr = ctx.Auth.GetIdentity()
	})
	if authErr == nil || authErr.Error() != "expired" || userID != "" {
		t.Fatalf("GetIdentity = %q, %v; want expired", userID, authErr)
	}

	plain := newTestEngine(t)
	callAction(plain, map[string]string{"Authorization": "Bearer anything"}, func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		userID, authErr = ctx.Auth.GetIdentity()
	})
	if authErr != nil || userID != "" {
		t.Fatalf("default auth GetIdentity = %q, %v; want anonymous", userID, authErr)
	}
}

func TestHTTPActionGuardUsesCallerAndRejectsWrites(t *testing.T) {
	e := newTestEngine(t)
	e.SetAuth(&headerAuth{userID: "alice"})
	e.RegisterGuard("isMember", func(ctx *GuardCtx) (any, error) {
		id, err := ctx.Auth.GetIdentity()
		if err != nil {
			return nil, err
		}
		if _, err := ctx.Auth.ExecuteGuard("other", nil); err == nil || err.Error() != "guards cannot execute other guards" {
			return nil, fmt.Errorf("nested guard error = %v", err)
		}
		writeErr := ctx.DB.Create(&testMessage{Body: "nope", RoomID: "lobby"}).Error
		if !errors.Is(writeErr, errReadOnly) {
			return nil, fmt.Errorf("guard write error = %v, want read-only", writeErr)
		}
		return id == "alice" && ctx.Params["room"] == "general", nil
	})

	var allowed any
	var guardErr error
	var missing error
	callAction(e, map[string]string{"Authorization": "Bearer token"}, func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		allowed, guardErr = ctx.Auth.ExecuteGuard("isMember", map[string]interface{}{"room": "general"})
		_, missing = ctx.Auth.ExecuteGuard("missing", nil)
	})
	if guardErr != nil || allowed != true {
		t.Fatalf("ExecuteGuard = %#v, %v; want true", allowed, guardErr)
	}
	if missing == nil || missing.Error() != "guard not found" {
		t.Fatalf("missing guard error = %v", missing)
	}
	var rows int64
	if err := e.db.Model(&testMessage{}).Count(&rows).Error; err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if rows != 0 {
		t.Fatalf("guard wrote %d messages", rows)
	}
}

func TestHTTPActionExecuteHasNoCaller(t *testing.T) {
	e := newTestEngine(t)
	e.SetAuth(&headerAuth{userID: "alice"})
	client := trackClient(t, e)

	var runs atomic.Int64
	e.RegisterQuery("getMessages", func(ctx *QueryCtx) (any, error) {
		runs.Add(1)
		ctx.TrackCollection("messages", "room_id", "lobby")
		var msgs []testMessage
		ctx.DB.Where("room_id = ?", "lobby").Find(&msgs)
		return len(msgs), nil
	})
	subscribe(t, e, client, "getMessages", "lobby", nil)
	drain(client)

	e.RegisterQuery("hiddenRead", func(ctx *QueryCtx) (any, error) {
		if _, err := ctx.Auth.GetIdentity(); !errors.Is(err, ErrNoCaller) {
			return nil, fmt.Errorf("GetIdentity error = %v, want ErrNoCaller", err)
		}
		if err := ctx.DB.Create(&testMessage{Body: "nope", RoomID: "lobby"}).Error; !errors.Is(err, errReadOnly) {
			return nil, fmt.Errorf("query write error = %v, want read-only", err)
		}
		return "read", nil
	}, Internal())
	e.RegisterMutation("hiddenWrite", func(ctx *MutationCtx) (any, error) {
		if _, err := ctx.Auth.GetIdentity(); !errors.Is(err, ErrNoCaller) {
			return nil, fmt.Errorf("GetIdentity error = %v, want ErrNoCaller", err)
		}
		if _, err := ctx.Auth.ExecuteGuard("unused", nil); err == nil || err.Error() != "guards cannot be executed internally" {
			return nil, fmt.Errorf("ExecuteGuard error = %v", err)
		}
		if err := ctx.DB.Create(&testMessage{Body: "hi", RoomID: "lobby"}).Error; err != nil {
			return nil, err
		}
		return "written", nil
	}, Internal())

	var readResult, writeResult any
	var readErr, writeErr, missingQuery, missingMutation error
	callAction(e, map[string]string{"Authorization": "Bearer token"}, func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		id, err := ctx.Auth.GetIdentity()
		if err != nil || id != "alice" {
			t.Errorf("action identity = %q, %v; want alice", id, err)
		}
		readResult, readErr = ctx.ExecuteQuery("hiddenRead", nil)
		writeResult, writeErr = ctx.ExecuteMutation("hiddenWrite", nil)
		_, missingQuery = ctx.ExecuteQuery("missing", nil)
		_, missingMutation = ctx.ExecuteMutation("missing", nil)
	})
	if readErr != nil || readResult != "read" {
		t.Fatalf("ExecuteQuery = %#v, %v", readResult, readErr)
	}
	if writeErr != nil || writeResult != "written" {
		t.Fatalf("ExecuteMutation = %#v, %v", writeResult, writeErr)
	}
	if missingQuery == nil || missingQuery.Error() != "query not found" {
		t.Fatalf("missing query error = %v", missingQuery)
	}
	if missingMutation == nil || missingMutation.Error() != "mutation not found" {
		t.Fatalf("missing mutation error = %v", missingMutation)
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("query runs = %d, want 2", got)
	}
	hidden := e.tracker.SubscribeToQuery(client.ID, "hiddenRead", "hidden", map[string]interface{}{})
	if _, err := e.executeQuery("hiddenRead", map[string]interface{}{}, hidden); err == nil || err.Error() != "query not found" {
		t.Fatalf("client call of internal query error = %v, want query not found", err)
	}
}

func TestHTTPActionCanScheduleAndStoreFiles(t *testing.T) {
	e, _ := newStorageEngine(t)
	e.RegisterMutation("later", func(ctx *MutationCtx) (any, error) { return nil, nil }, Internal())

	var taskID, fileID, uploadURL, hookFile string
	var scheduleErr, putErr, uploadErr, hookErr error
	callAction(e, nil, func(ctx *ActionCtx, w http.ResponseWriter, r *http.Request) {
		taskID, scheduleErr = ctx.Scheduler.RunAfter(time.Hour, "later", map[string]interface{}{"n": 1})
		fileID, putErr = ctx.Storage.PutFile("text/plain", strings.NewReader("from-action"), storage.Public())
		info, err := ctx.Storage.GetUploadURL()
		uploadErr = err
		if err == nil {
			uploadURL = info.UploadURL
			hookFile = info.FileID
			hookErr = ctx.Storage.RunAfterUpload(info.FileID, "later", map[string]interface{}{"room": "general"})
		}
	})
	if scheduleErr != nil || taskID == "" {
		t.Fatalf("RunAfter = %q, %v", taskID, scheduleErr)
	}
	var task TetherTask
	if err := e.db.First(&task, "id = ?", taskID).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.FunctionName != "later" || task.ParamsJSON != `{"n":1}` {
		t.Fatalf("task = %+v, want later with n=1", task)
	}
	if putErr != nil || fileID == "" {
		t.Fatalf("PutFile = %q, %v", fileID, putErr)
	}
	got := serveStorage(e, http.MethodGet, "/storage/public/"+fileID, nil, nil)
	if got.Code != http.StatusOK || got.Body.String() != "from-action" {
		t.Fatalf("public file status = %d, body %q", got.Code, got.Body.String())
	}
	if uploadErr != nil || !strings.HasPrefix(uploadURL, "/storage/upload/") {
		t.Fatalf("GetUploadURL = %q, %v", uploadURL, uploadErr)
	}
	if hookErr != nil {
		t.Fatalf("RunAfterUpload: %v", hookErr)
	}
	var hook TetherUploadHook
	if err := e.db.Where("file_id = ?", hookFile).First(&hook).Error; err != nil {
		t.Fatalf("load hook: %v", err)
	}
	if hook.MutationName != "later" || hook.ParamsJSON != `{"room":"general"}` {
		t.Fatalf("hook = %+v", hook)
	}
}

func TestRunAfterUploadRunsMutationWithStorageAndMetadata(t *testing.T) {
	e, _ := newStorageEngine(t)
	got := make(chan map[string]interface{}, 1)
	e.RegisterMutation("onUpload", func(ctx *MutationCtx) (any, error) {
		if _, err := ctx.Auth.GetIdentity(); !errors.Is(err, ErrNoCaller) {
			return nil, fmt.Errorf("GetIdentity error = %v, want ErrNoCaller", err)
		}
		got <- ctx.Params
		return nil, nil
	}, Internal())
	e.RegisterMutation("prepare", func(ctx *MutationCtx) (any, error) {
		info, err := ctx.Storage.GetUploadURL(storage.Public())
		if err != nil {
			return nil, err
		}
		if err := ctx.Storage.RunAfterUpload(info.FileID, "onUpload", map[string]interface{}{
			"room":           "general",
			"n":              2,
			"storage.fileID": "replaced",
		}); err != nil {
			return nil, err
		}
		return info, nil
	}, Internal())

	saved, err := e.ExecuteMutation("prepare", nil)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	info, ok := saved.(storage.UploadInfo)
	if !ok {
		t.Fatalf("prepare result = %#v, want UploadInfo", saved)
	}
	var row TetherStorage
	if err := e.db.Where("id = ?", info.FileID).First(&row).Error; err != nil {
		t.Fatalf("load upload: %v", err)
	}

	other, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("other getUploadURL: %v", err)
	}
	if err := e.runAfterUpload(other.FileID, "onUpload", map[string]interface{}{"room": "other"}); err != nil {
		t.Fatalf("hook other file: %v", err)
	}

	const fileBody = "hello"
	put := serveStorage(e, http.MethodPut, info.UploadURL, strings.NewReader(fileBody), map[string]string{
		"Content-Type":         "text/plain",
		"X-Tether-Meta-Room":   "lobby",
		"X-Tether-Meta-Author": "ada",
	})
	if put.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body %s", put.Code, put.Body.String())
	}
	var left int64
	if err := e.db.Model(&TetherUploadHook{}).Where("file_id = ?", info.FileID).Count(&left).Error; err != nil {
		t.Fatalf("count finished hook: %v", err)
	}
	if left != 0 {
		t.Fatalf("finished hook rows = %d, want 0", left)
	}

	params := waitForParams(t, got)
	if params["room"] != "general" || params["n"] != float64(2) {
		t.Fatalf("caller params = %#v, want room general and n 2", params)
	}
	if params["storage.fileID"] != info.FileID {
		t.Fatalf("storage.fileID = %#v, want %s", params["storage.fileID"], info.FileID)
	}
	if params["storage.fileSize"] != int64(len(fileBody)) {
		t.Fatalf("storage.fileSize = %#v (%T), want %d", params["storage.fileSize"], params["storage.fileSize"], len(fileBody))
	}
	if params["storage.mimeType"] != "text/plain" || params["storage.public"] != true {
		t.Fatalf("storage type/public = %#v %#v", params["storage.mimeType"], params["storage.public"])
	}
	expiresAt, ok := params["storage.expiresAt"].(time.Time)
	createdAt, createdOK := params["storage.createdAt"].(time.Time)
	if !ok || !createdOK || !expiresAt.Equal(row.ExpiresAt) || !createdAt.Equal(row.CreatedAt) {
		t.Fatalf("storage times = %#v %#v, want %s %s", params["storage.expiresAt"], params["storage.createdAt"], row.ExpiresAt, row.CreatedAt)
	}
	if params["metadata.room"] != "lobby" || params["metadata.author"] != "ada" {
		t.Fatalf("metadata = room %#v author %#v", params["metadata.room"], params["metadata.author"])
	}
	select {
	case extra := <-got:
		t.Fatalf("unexpected mutation params %#v", extra)
	default:
	}
	if err := e.db.Where("file_id = ?", other.FileID).First(&TetherUploadHook{}).Error; err != nil {
		t.Fatalf("other file's hook was removed: %v", err)
	}
}

func TestRunAfterUploadNilParamsAndSingleHook(t *testing.T) {
	e, _ := newStorageEngine(t)
	got := make(chan map[string]interface{}, 1)
	e.RegisterMutation("onUpload", func(ctx *MutationCtx) (any, error) {
		got <- ctx.Params
		return nil, nil
	}, Internal())

	info, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	if err := e.runAfterUpload(info.FileID, "onUpload", nil); err != nil {
		t.Fatalf("RunAfterUpload nil params: %v", err)
	}
	if err := e.runAfterUpload(info.FileID, "onUpload", map[string]interface{}{"again": true}); err == nil {
		t.Fatal("second hook for the same file succeeded, want an error")
	}

	put := serveStorage(e, http.MethodPut, info.UploadURL, strings.NewReader("x"), nil)
	if put.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body %s", put.Code, put.Body.String())
	}
	params := waitForParams(t, got)
	if params["storage.fileID"] != info.FileID || params["storage.mimeType"] != "application/octet-stream" {
		t.Fatalf("params = %#v, want file id and default mime type", params)
	}
	if _, ok := params["again"]; ok {
		t.Fatal("second hook's params were stored")
	}
}

func TestRunAfterUploadRequiresStorageAndIsDeniedInQueries(t *testing.T) {
	e := newTestEngine(t)
	e.RegisterMutation("save", func(ctx *MutationCtx) (any, error) {
		return nil, ctx.Storage.RunAfterUpload("file", "later", nil)
	}, Internal())
	if _, err := e.ExecuteMutation("save", nil); err == nil {
		t.Fatal("RunAfterUpload without storage returned nil")
	}

	storeEngine, store := newStorageEngine(t)
	storeEngine.RegisterQuery("save", func(ctx *QueryCtx) (any, error) {
		return nil, ctx.Storage.RunAfterUpload("file", "later", nil)
	})
	client := trackClient(t, storeEngine)
	sub := storeEngine.tracker.SubscribeToQuery(client.ID, "save", "k", map[string]interface{}{})
	raw, err := storeEngine.executeQuery("save", map[string]interface{}{}, sub)
	if err != nil {
		t.Fatalf("executeQuery: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("decode query frame: %v", err)
	}
	if msg["type"] != "error" || msg["error"] != "tether: this capability is not available in this context" {
		t.Fatalf("client query frame = %#v, want capability denied", msg)
	}
	if _, err := storeEngine.ExecuteQuery("save", nil); err == nil || err.Error() != "tether: this capability is not available in this context" {
		t.Fatalf("ExecuteQuery RunAfterUpload error = %v, want capability denied", err)
	}
	var hooks int64
	if err := storeEngine.db.Model(&TetherUploadHook{}).Count(&hooks).Error; err != nil {
		t.Fatalf("count hooks: %v", err)
	}
	if hooks != 0 {
		t.Fatalf("query registered %d hooks", hooks)
	}
	if entries, err := os.ReadDir(store.UploadDir); err != nil {
		t.Fatalf("read upload dir: %v", err)
	} else if len(entries) != 0 {
		t.Fatalf("query wrote %d files", len(entries))
	}
}

func TestRunAfterUploadFailureStillCompletesUpload(t *testing.T) {
	e, _ := newStorageEngine(t)
	ran := make(chan struct{})
	e.RegisterMutation("onUpload", func(ctx *MutationCtx) (any, error) {
		close(ran)
		return nil, errors.New("nope")
	}, Internal())
	info, err := e.getUploadURL()
	if err != nil {
		t.Fatalf("getUploadURL: %v", err)
	}
	if err := e.runAfterUpload(info.FileID, "onUpload", map[string]interface{}{}); err != nil {
		t.Fatalf("RunAfterUpload: %v", err)
	}

	missing := serveStorage(e, http.MethodPut, "/storage/upload/not-a-token", strings.NewReader("nope"), nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("bad token status = %d, want 404", missing.Code)
	}
	select {
	case <-ran:
		t.Fatal("mutation ran for a rejected upload")
	default:
	}
	if err := e.db.Where("file_id = ?", info.FileID).First(&TetherUploadHook{}).Error; err != nil {
		t.Fatalf("hook removed after rejected upload: %v", err)
	}

	put := serveStorage(e, http.MethodPut, info.UploadURL, strings.NewReader("ok"), map[string]string{
		"Content-Type": "text/plain",
	})
	if put.Code != http.StatusOK {
		t.Fatalf("upload status = %d, body %s", put.Code, put.Body.String())
	}
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for failing post-upload mutation")
	}
	var left int64
	if err := e.db.Model(&TetherUploadHook{}).Where("file_id = ?", info.FileID).Count(&left).Error; err != nil {
		t.Fatalf("count hook: %v", err)
	}
	if left != 0 {
		t.Fatalf("hook rows after failed mutation = %d, want 0", left)
	}
}
