package reactivity

import (
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"sort"
	"strings"

	"github.com/cespare/xxhash"
)

// TODO: populate with needed structures for tracking state
type Tracker struct {
	mu sync.RWMutex

	// Maps a Client's UUID to their actual Client struct
	clients map[string]*Client

	subscriptions map[string]*Subscription       // sub ID -> subscriptions
	clientToSubs  map[string]map[string]struct{} // client ID -> sub IDs
	subToTags     map[string]map[string]struct{} // sub ID -> tags
	tagsToSubs    map[string]map[string]struct{} // tags -> sub IDs

	// depsAt is the newest execution timestamp whose dependency tags were
	// stored for a subscription. guardAt is that timestamp per guard-result
	// prefix. An older execution must not replace either.
	depsAt  map[string]int64
	guardAt map[string]map[string]int64
}

type Subscription struct {
	SubID        string
	Client       *Client
	Query        string
	LinkedSubIDs []string
	QueryKey     string // provided by the client to help with caching
	ParamsHash   string // used for batching/deduplication on tag invalidation
	Params       map[string]interface{}
}

func NewTracker() *Tracker {
	return &Tracker{
		clients:       make(map[string]*Client),
		subscriptions: make(map[string]*Subscription),
		clientToSubs:  make(map[string]map[string]struct{}),
		subToTags:     make(map[string]map[string]struct{}),
		tagsToSubs:    make(map[string]map[string]struct{}),
		depsAt:        make(map[string]int64),
		guardAt:       make(map[string]map[string]int64),
	}
}

// resetAuthorization drops the client's guard subscriptions and every
// permanent (*) tag on its query subscriptions, since those encode decisions
// made under the previous identity. It returns the query subscriptions so the
// caller can re-run them. Callers must hold t.mu.
func (t *Tracker) resetAuthorization(clientID string) []*Subscription {
	subscriptions := make([]*Subscription, 0, len(t.clientToSubs[clientID]))
	for subID := range t.clientToSubs[clientID] {
		sub, ok := t.subscriptions[subID]
		if !ok {
			continue
		}
		for _, guardID := range sub.LinkedSubIDs {
			t.removeSubscription(guardID)
		}
		sub.LinkedSubIDs = nil
		for tag := range t.subToTags[subID] {
			if !strings.HasPrefix(tag, "*") {
				continue
			}
			delete(t.subToTags[subID], tag)
			delete(t.tagsToSubs[tag], subID)
			if len(t.tagsToSubs[tag]) == 0 {
				delete(t.tagsToSubs, tag)
			}
		}
		delete(t.guardAt, subID)
		subscriptions = append(subscriptions, sub)
	}
	return subscriptions
}

// authEpochCurrent reports whether sub's client is still tracked and still on
// authEpoch. Guard subscriptions resolve their client through the attached
// query. Callers must hold t.mu.
func (t *Tracker) authEpochCurrent(sub *Subscription, authEpoch int) bool {
	client := sub.Client
	if client == nil && len(sub.LinkedSubIDs) > 0 {
		if attached, ok := t.subscriptions[sub.LinkedSubIDs[0]]; ok {
			client = attached.Client
		}
	}
	if client == nil || t.clients[client.ID] != client {
		return false
	}
	return client.GetAuth().AuthEpoch == authEpoch
}

// AttachGuardToSubscription records a guard's first execution as its own
// subscription with deps. It is dropped if the client's identity changed since
// authEpoch was read, or if a newer execution already published state for this
// query. A newer execution replaces an older attachment of the same guard.
func (t *Tracker) AttachGuardToSubscription(subID string, guardID string, guardName string, params map[string]interface{}, deps []string, authEpoch int, ts int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	querySub, ok := t.subscriptions[subID]
	if !ok {
		slog.Error("Tracker: Subscription not found", "subID", subID)
		return
	}
	if !t.authEpochCurrent(querySub, authEpoch) {
		slog.Debug("Tracker: Dropping guard from a previous identity", "subID", subID, "guard", guardName)
		return
	}
	if ts < t.depsAt[subID] {
		slog.Debug("Tracker: Dropping stale guard attachment", "subID", subID, "guard", guardName)
		return
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		slog.Error("Tracker: Failed to marshal params", "error", err)
		return
	}
	paramsHash := strconv.FormatUint(xxhash.Sum64(paramsJSON), 10)
	if existing := t.linkedGuard(querySub, guardName, paramsHash); existing != nil {
		if t.depsAt[existing.SubID] > ts {
			slog.Debug("Tracker: Dropping stale guard attachment", "subID", subID, "guard", guardName)
			return
		}
		t.unlinkGuard(querySub, existing.SubID)
	}
	t.subscriptions[guardID] = &Subscription{
		SubID:        guardID,
		Client:       nil,
		Query:        guardName,
		LinkedSubIDs: []string{subID},
		QueryKey:     "",
		ParamsHash:   paramsHash,
		Params:       params,
	}
	querySub.LinkedSubIDs = append(querySub.LinkedSubIDs, guardID)
	t.commitTags(guardID, deps, ts)
	if ts > t.depsAt[subID] {
		t.depsAt[subID] = ts
	}
}

// linkedGuard returns the guard subscription already attached to querySub for
// the same guard name and params. Callers must hold t.mu.
func (t *Tracker) linkedGuard(querySub *Subscription, guardName, paramsHash string) *Subscription {
	for _, id := range querySub.LinkedSubIDs {
		guard, ok := t.subscriptions[id]
		if !ok {
			continue
		}
		if guard.Query == guardName && guard.ParamsHash == paramsHash {
			return guard
		}
	}
	return nil
}

// unlinkGuard removes guardID from querySub and from the tracker. Callers must
// hold t.mu.
func (t *Tracker) unlinkGuard(querySub *Subscription, guardID string) {
	kept := make([]string, 0, len(querySub.LinkedSubIDs))
	for _, id := range querySub.LinkedSubIDs {
		if id != guardID {
			kept = append(kept, id)
		}
	}
	querySub.LinkedSubIDs = kept
	t.removeSubscription(guardID)
}

func (t *Tracker) Track(c *Client) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clients[c.ID] = c
}

func (t *Tracker) Untrack(c *Client) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.clients[c.ID]; !exists {
		slog.Error("Tracker: Client not found", "clientID", c.ID)
		return
	}

	for subID := range t.clientToSubs[c.ID] {
		if sub, ok := t.subscriptions[subID]; ok {
			for _, linkedID := range append([]string(nil), sub.LinkedSubIDs...) {
				t.removeSubscription(linkedID)
			}
		}
		t.removeSubscription(subID)
	}
	delete(t.clientToSubs, c.ID)
	delete(t.clients, c.ID)
	c.SetExpiryTimer(nil)
	slog.Debug("Tracker: Untracked client", "clientID", c.ID)
}

func (t *Tracker) removeSubscription(subID string) {
	sub, exists := t.subscriptions[subID]
	if !exists {
		return
	}

	// 1. Remove this sub from all tags it was listening to
	for tag := range t.subToTags[subID] {
		delete(t.tagsToSubs[tag], subID)
		// Clean up empty tag maps
		if len(t.tagsToSubs[tag]) == 0 {
			delete(t.tagsToSubs, tag)
		}
	}

	// 2. Clear the sub's tag list
	delete(t.subToTags, subID)

	// 3. Remove from the client's personal list
	if sub.Client != nil {
		delete(t.clientToSubs[sub.Client.ID], subID)
	}

	// 4. Finally, delete the subscription itself
	delete(t.subscriptions, subID)
	delete(t.depsAt, subID)
	delete(t.guardAt, subID)
}

// SetAuth updates the client's identity. When the user ID changes, cached
// authorization state is reset and the client's query subscriptions are
// returned so the caller can re-run them under the new identity.
func (t *Tracker) SetAuth(clientID string, userID string, expiresAt time.Time) []*Subscription {
	t.mu.Lock()
	defer t.mu.Unlock()
	client, exists := t.clients[clientID]
	if !exists {
		slog.Error("Tracker: Client not found", "clientID", clientID)
		return nil
	}
	changed := client.GetAuth().UserID != userID
	client.SetAuth(userID, expiresAt)
	if !changed {
		return nil
	}
	return t.resetAuthorization(clientID)
}

// ExpireAuth clears the client's identity if it still holds the credential
// expiring at expiresAt, resetting authorization state like SetAuth.
func (t *Tracker) ExpireAuth(clientID string, expiresAt time.Time) []*Subscription {
	t.mu.Lock()
	defer t.mu.Unlock()
	client, exists := t.clients[clientID]
	if !exists {
		return nil
	}
	auth := client.GetAuth()
	if !auth.ExpiresAt.Equal(expiresAt) {
		return nil
	}
	client.SetAuth("", time.Time{})
	if auth.UserID == "" {
		return nil
	}
	// send is used directly because ExpireAuth already holds t.mu. SendMessage
	// would take that lock again and stall the expiry rerun.
	t.send(client, []byte(`{"type": "auth", "success": false, "data": "Identity expired"}`))
	return t.resetAuthorization(clientID)
}

// ArmAuthExpiry replaces the client's auth-expiry timer with the one returned
// by arm. arm runs under t.mu, so a caller that checks for shutdown inside arm
// cannot store a timer after StopAuthExpiryTimers has swept the clients. arm
// may return nil to leave the client without a timer.
func (t *Tracker) ArmAuthExpiry(clientID string, arm func() *time.Timer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	client, exists := t.clients[clientID]
	if !exists {
		return
	}
	client.SetExpiryTimer(arm())
}

// StopAuthExpiryTimers stops the auth-expiry timer of every tracked client.
func (t *Tracker) StopAuthExpiryTimers() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, client := range t.clients {
		client.SetExpiryTimer(nil)
	}
}

func (t *Tracker) GetAuth(clientID string) (AuthCtx, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if client, exists := t.clients[clientID]; exists {
		return client.GetAuth(), true
	}
	slog.Error("Tracker: Client not found", "clientID", clientID)
	return AuthCtx{}, false
}

func (t *Tracker) GetSubscription(subID string) (*Subscription, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if sub, exists := t.subscriptions[subID]; exists {
		return sub, true
	}
	return nil, false
}

func (t *Tracker) SubscribeToQuery(clientID string, query string, queryKey string, params map[string]interface{}) *Subscription {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.clients[clientID]; !exists {
		slog.Error("Tracker: Client not found", "clientID", clientID)
		return nil
	}

	if len(t.clientToSubs[clientID]) >= 100 {
		slog.Error("Tracker: Client has too many subscriptions", "clientID", clientID)
		return nil
	}

	paramsJSON, err := json.Marshal(params)
	if err != nil {
		slog.Error("Tracker: Failed to marshal params", "error", err)
		return nil
	}
	subID := clientID + "-" + query + "-" + strconv.FormatUint(xxhash.Sum64(paramsJSON), 10)
	if _, exists := t.subscriptions[subID]; exists {
		slog.Debug("Tracker: Subscription already exists", "subID", subID)
		return t.subscriptions[subID]
	}
	if _, exists := t.clientToSubs[clientID]; !exists {
		t.clientToSubs[clientID] = make(map[string]struct{})
	}

	sub := &Subscription{
		SubID:      subID,
		Client:     t.clients[clientID],
		Query:      query,
		QueryKey:   queryKey,
		ParamsHash: strconv.FormatUint(xxhash.Sum64(paramsJSON), 10),
		Params:     params,
	}
	t.subscriptions[subID] = sub
	t.clientToSubs[clientID][subID] = struct{}{}
	slog.Debug("Tracker: Subscribed to query", "query", query, "clientID", clientID, "params", params)
	return sub
}

func (t *Tracker) UnsubscribeFromQuery(clientID string, query string, params map[string]interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		slog.Error("Tracker: Failed to marshal params", "error", err)
		return
	}
	subID := clientID + "-" + query + "-" + strconv.FormatUint(xxhash.Sum64(paramsJSON), 10)
	if _, exists := t.clientToSubs[clientID][subID]; !exists {
		slog.Debug("Tracker: Subscription not found", "subID", subID)
		return
	}
	if sub, ok := t.subscriptions[subID]; ok {
		for _, linkedID := range append([]string(nil), sub.LinkedSubIDs...) {
			t.removeSubscription(linkedID)
		}
	}
	t.removeSubscription(subID)
	slog.Debug("Tracker: Unsubscribed from query", "query", query, "clientID", clientID, "params", params)
}

func (t *Tracker) UpdateTags(subID string, newTags []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.subscriptions[subID]; !exists {
		slog.Error("Tracker: Subscription not found", "subID", subID)
		return
	}
	t.updateTags(subID, newTags)
}

// UpdateTagsAtEpoch is UpdateTags for work that ran under the client identity
// at authEpoch and execution timestamp ts. It reports false and changes nothing
// if that identity is gone or a newer execution already stored dependencies.
func (t *Tracker) UpdateTagsAtEpoch(subID string, newTags []string, authEpoch int, ts int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	sub, exists := t.subscriptions[subID]
	if !exists || !t.authEpochCurrent(sub, authEpoch) {
		return false
	}
	if !t.commitTags(subID, newTags, ts) {
		slog.Debug("Tracker: Dropping stale dependency tags", "subID", subID)
		return false
	}
	return true
}

// CommitQueryResult records a query execution's tags and sends its message,
// unless the client's identity changed since authEpoch was read. Both happen
// under one lock so an auth reset cannot land between them. Tags from an
// execution older than the one already recorded are left unchanged; the
// message is still delivered so the client can order frames.
func (t *Tracker) CommitQueryResult(subID string, newTags []string, message []byte, authEpoch int, ts int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	sub, exists := t.subscriptions[subID]
	if !exists || sub.Client == nil || !t.authEpochCurrent(sub, authEpoch) {
		return false
	}
	if !t.commitTags(subID, newTags, ts) {
		slog.Debug("Tracker: Dropping stale dependency tags", "subID", subID)
	}
	t.send(sub.Client, message)
	return true
}

// commitTags stores newTags when ts is at least as new as the dependency
// state already recorded for subID. A guard result published by a newer
// execution is left in place. Callers must hold t.mu and have checked that
// subID exists. It reports whether this execution's tags were stored.
func (t *Tracker) commitTags(subID string, newTags []string, ts int64) bool {
	if ts < t.depsAt[subID] {
		return false
	}
	filtered := make([]string, 0, len(newTags))
	writtenGuards := make([]string, 0)
	for _, tag := range newTags {
		prefix := guardResultPrefix(tag)
		if prefix != "" && t.guardRevision(subID, prefix) > ts {
			continue
		}
		filtered = append(filtered, tag)
		if prefix != "" {
			writtenGuards = append(writtenGuards, prefix)
		}
	}
	t.updateTags(subID, filtered)
	t.depsAt[subID] = ts
	for _, prefix := range writtenGuards {
		t.setGuardRevision(subID, prefix, ts)
	}
	return true
}

func (t *Tracker) guardRevision(subID, prefix string) int64 {
	return t.guardAt[subID][prefix]
}

func (t *Tracker) setGuardRevision(subID, prefix string, ts int64) {
	if ts < t.guardRevision(subID, prefix) {
		return
	}
	if t.guardAt[subID] == nil {
		t.guardAt[subID] = make(map[string]int64)
	}
	t.guardAt[subID][prefix] = ts
}

// guardResultPrefix is the name-and-params identity of a cached guard result,
// "*guard_<name>_<paramsHash>:". Other tags return "".
func guardResultPrefix(tag string) string {
	if !strings.HasPrefix(tag, "*guard_") {
		return ""
	}
	colon := strings.IndexByte(tag, ':')
	if colon < 0 {
		return ""
	}
	return tag[:colon+1]
}

// updateTags replaces subID's non-permanent tags with newTags. Permanent tags
// stay, except a guard result, which replaces any result already stored for
// the same guard name and params. Callers must hold t.mu and have checked
// that subID exists.
func (t *Tracker) updateTags(subID string, newTags []string) {
	// Collect permanent tags before removing old tags
	permanentTags := make(map[string]struct{})
	for tag := range t.subToTags[subID] {
		if len(tag) > 0 && tag[0] == '*' {
			permanentTags[tag] = struct{}{}
		}
	}

	// One result per guard name and params. The last result in this update
	// wins, including over a result already recorded on the subscription.
	guardIndex := make(map[string]int)
	collapsed := make([]string, 0, len(newTags))
	for _, tag := range newTags {
		prefix := guardResultPrefix(tag)
		if prefix == "" {
			collapsed = append(collapsed, tag)
			continue
		}
		if idx, ok := guardIndex[prefix]; ok {
			collapsed[idx] = tag
			continue
		}
		guardIndex[prefix] = len(collapsed)
		collapsed = append(collapsed, tag)
	}
	for prefix := range guardIndex {
		for old := range permanentTags {
			if strings.HasPrefix(old, prefix) {
				delete(permanentTags, old)
			}
		}
	}
	newTags = collapsed

	// Remove subID from non-permanent tags in tagsToSubs
	for oldTag := range t.subToTags[subID] {
		if _, isPermanent := permanentTags[oldTag]; isPermanent {
			continue // Do not remove subID from permanent tags
		}
		delete(t.tagsToSubs[oldTag], subID)
		if len(t.tagsToSubs[oldTag]) == 0 {
			delete(t.tagsToSubs, oldTag)
		}
	}

	// Reset subToTags for this subID, but keep permanent tags
	t.subToTags[subID] = make(map[string]struct{})
	for tag := range permanentTags {
		t.subToTags[subID][tag] = struct{}{}
	}

	// Add new tags (including * tags, but safe to re-add)
	for _, newTag := range newTags {
		if _, exists := t.tagsToSubs[newTag]; !exists {
			t.tagsToSubs[newTag] = make(map[string]struct{})
		}
		t.tagsToSubs[newTag][subID] = struct{}{}
		t.subToTags[subID][newTag] = struct{}{}
	}
}

// SubscriptionHasTag reports whether subID currently tracks tag.
func (t *Tracker) SubscriptionHasTag(subID, tag string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.subToTags[subID][tag]
	return ok
}

func (t *Tracker) GetAuthFingerprint(sub *Subscription) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var authTags []string
	for tag := range t.subToTags[sub.SubID] {
		if strings.HasPrefix(tag, "*") {
			authTags = append(authTags, tag)
		}
	}
	sort.Strings(authTags)
	return strings.Join(authTags, "|")
}

func (t *Tracker) GetGuardFingerprint(sub *Subscription, guardName string, paramsHash string) string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for tag := range t.subToTags[sub.SubID] {
		if strings.HasPrefix(tag, "*guard_"+guardName+"_"+paramsHash+":") {
			return tag
		}
	}
	return ""
}

// UpdateGuardFingerprint replaces the cached guard result on subID. It reports
// whether the fingerprint changed, and changes nothing if the client's identity
// changed since authEpoch was read or a newer execution already published this
// guard result.
func (t *Tracker) UpdateGuardFingerprint(subID string, guardName string, paramsHash string, newValue string, authEpoch int, ts int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	sub, exists := t.subscriptions[subID]
	if !exists {
		slog.Error("Tracker: Subscription not found", "subID", subID)
		return false
	}
	if !t.authEpochCurrent(sub, authEpoch) {
		slog.Debug("Tracker: Dropping guard fingerprint from a previous identity", "subID", subID, "guard", guardName)
		return false
	}
	if t.subToTags[subID] == nil {
		t.subToTags[subID] = make(map[string]struct{})
	}

	prefix := "*guard_" + guardName + "_" + paramsHash + ":"
	if t.guardRevision(subID, prefix) > ts {
		slog.Debug("Tracker: Dropping stale guard fingerprint", "subID", subID, "guard", guardName)
		return false
	}
	t.setGuardRevision(subID, prefix, ts)
	newTag := prefix + newValue
	var oldTag string
	for tag := range t.subToTags[subID] {
		if strings.HasPrefix(tag, prefix) {
			oldTag = tag
			break
		}
	}
	if oldTag == newTag {
		return false
	}
	if oldTag != "" {
		delete(t.subToTags[subID], oldTag)
		delete(t.tagsToSubs[oldTag], subID)
		if len(t.tagsToSubs[oldTag]) == 0 {
			delete(t.tagsToSubs, oldTag)
		}
	}
	t.subToTags[subID][newTag] = struct{}{}
	if _, exists := t.tagsToSubs[newTag]; !exists {
		t.tagsToSubs[newTag] = make(map[string]struct{})
	}
	t.tagsToSubs[newTag][subID] = struct{}{}
	return true
}

// DropGuards discards every cached guard result on the query attached to
// guardID, along with its guard subscriptions, so the query's next run must
// execute its guards again. It is a no-op if guardID is no longer tracked or
// a newer execution has already published this query's guard state. ts is the
// execution that decided the guard could not be revalidated; zero means the
// caller is unversioned and the drop always applies.
func (t *Tracker) DropGuards(guardID string, ts int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	guard, ok := t.subscriptions[guardID]
	if !ok || len(guard.LinkedSubIDs) == 0 {
		return
	}
	querySub, ok := t.subscriptions[guard.LinkedSubIDs[0]]
	if !ok {
		t.removeSubscription(guardID)
		return
	}
	if ts > 0 && t.executionSuperseded(querySub, ts) {
		slog.Debug("Tracker: Keeping newer guard state", "subID", querySub.SubID, "guard", guardID)
		return
	}
	for _, linkedID := range querySub.LinkedSubIDs {
		t.removeSubscription(linkedID)
	}
	querySub.LinkedSubIDs = nil
	for tag := range t.subToTags[querySub.SubID] {
		if !strings.HasPrefix(tag, "*guard_") {
			continue
		}
		prefix := guardResultPrefix(tag)
		delete(t.subToTags[querySub.SubID], tag)
		delete(t.tagsToSubs[tag], querySub.SubID)
		if len(t.tagsToSubs[tag]) == 0 {
			delete(t.tagsToSubs, tag)
		}
		if prefix != "" && ts > 0 {
			t.setGuardRevision(querySub.SubID, prefix, ts)
		}
	}
}

// executionSuperseded reports whether a newer execution than ts has already
// published dependency or guard state for querySub. Callers must hold t.mu.
func (t *Tracker) executionSuperseded(querySub *Subscription, ts int64) bool {
	if t.depsAt[querySub.SubID] > ts {
		return true
	}
	for _, id := range querySub.LinkedSubIDs {
		if t.depsAt[id] > ts {
			return true
		}
	}
	for _, at := range t.guardAt[querySub.SubID] {
		if at > ts {
			return true
		}
	}
	return false
}

func (t *Tracker) GetSubscriptionsToTag(tag string) []*Subscription {
	t.mu.RLock()
	defer t.mu.RUnlock()
	subscriptions := make([]*Subscription, 0, len(t.tagsToSubs[tag]))
	for subID := range t.tagsToSubs[tag] {
		sub := t.subscriptions[subID]
		if sub == nil {
			continue
		}
		subscriptions = append(subscriptions, sub)
	}
	return subscriptions
}

func (t *Tracker) SendMessage(clientID string, message []byte) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	client := t.clients[clientID]
	if client == nil {
		slog.Error("Tracker: Client not found", "clientID", clientID)
		return
	}
	t.send(client, message)
}

// send queues message without blocking. Callers must hold t.mu.
func (t *Tracker) send(client *Client, message []byte) {
	select {
	case client.Send <- message:
	default:
		slog.Error("Tracker: Client send channel is full", "clientID", client.ID)
		client.Conn.Close() // Aggressively close the connection to force the client to reconnect
	}
}
