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
}

type Subscription struct {
	SubID      string
	Client     *Client
	Query      string
	QueryKey   string // provided by the client to help with caching
	ParamsHash string // used for batching/deduplication on tag invalidation
	Params     map[string]interface{}
}

func NewTracker() *Tracker {
	return &Tracker{
		clients:       make(map[string]*Client),
		subscriptions: make(map[string]*Subscription),
		clientToSubs:  make(map[string]map[string]struct{}),
		subToTags:     make(map[string]map[string]struct{}),
		tagsToSubs:    make(map[string]map[string]struct{}),
	}
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
		t.removeSubscription(subID)
	}
	delete(t.clientToSubs, c.ID)
	delete(t.clients, c.ID)
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
	delete(t.clientToSubs[sub.Client.ID], subID)

	// 4. Finally, delete the subscription itself
	delete(t.subscriptions, subID)
}

func (t *Tracker) SetAuth(clientID string, userID string, expiresAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if client, exists := t.clients[clientID]; exists {
		client.SetAuth(userID, expiresAt)
	} else {
		slog.Error("Tracker: Client not found", "clientID", clientID)
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

func (t *Tracker) SubscribeToQuery(clientID string, query string, queryKey string, params map[string]interface{}) *Subscription {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.clients[clientID]; !exists {
		slog.Error("Tracker: Client not found", "clientID", clientID)
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

	// Collect permanent tags before removing old tags
	permanentTags := make(map[string]struct{})
	for tag := range t.subToTags[subID] {
		if len(tag) > 0 && tag[0] == '*' {
			permanentTags[tag] = struct{}{}
		}
	}

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

func (t *Tracker) GetSubscriptionsToTag(tag string) []*Subscription {
	t.mu.RLock()
	defer t.mu.RUnlock()
	subscriptions := make([]*Subscription, 0, len(t.tagsToSubs[tag]))
	for subID := range t.tagsToSubs[tag] {
		subscriptions = append(subscriptions, t.subscriptions[subID])
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
	select {
	case client.Send <- message:
	default:
		slog.Error("Tracker: Client send channel is full", "clientID", clientID)
	}
}
