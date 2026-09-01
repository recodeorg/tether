package reactivity

import (
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
)

func assertTrackerEmpty(t *testing.T, tr *Tracker) {
	t.Helper()
	if len(tr.clients) != 0 {
		t.Errorf("clients not empty: got %d entries", len(tr.clients))
	}
	if len(tr.subscriptions) != 0 {
		t.Errorf("subscriptions not empty: got %d entries", len(tr.subscriptions))
	}
	if len(tr.clientToSubs) != 0 {
		t.Errorf("clientToSubs not empty: got %d entries", len(tr.clientToSubs))
	}
	if len(tr.subToTags) != 0 {
		t.Errorf("subToTags not empty: got %d entries", len(tr.subToTags))
	}
	if len(tr.tagsToSubs) != 0 {
		t.Errorf("tagsToSubs not empty: got %d entries", len(tr.tagsToSubs))
	}
}

func assertTrackerConsistent(t *testing.T, tr *Tracker) {
	t.Helper()

	for id, client := range tr.clients {
		if client == nil {
			t.Errorf("clients[%q] is nil", id)
			continue
		}
		if client.ID != id {
			t.Errorf("clients[%q] has client.ID %q", id, client.ID)
		}
	}

	for subID, sub := range tr.subscriptions {
		if sub == nil {
			t.Errorf("subscriptions[%q] is nil", subID)
			continue
		}
		if sub.SubID != subID {
			t.Errorf("subscriptions[%q] has SubID %q", subID, sub.SubID)
		}
		if sub.Client == nil {
			t.Errorf("subscriptions[%q] has nil Client", subID)
			continue
		}
		if _, ok := tr.clients[sub.Client.ID]; !ok {
			t.Errorf("subscriptions[%q] references client %q which is not in clients", subID, sub.Client.ID)
		}
		if _, ok := tr.clientToSubs[sub.Client.ID][subID]; !ok {
			t.Errorf("subscriptions[%q] is missing from clientToSubs[%q]", subID, sub.Client.ID)
		}
	}

	for clientID, subIDs := range tr.clientToSubs {
		if _, ok := tr.clients[clientID]; !ok {
			t.Errorf("clientToSubs has client %q which is not in clients", clientID)
		}
		for subID := range subIDs {
			if _, ok := tr.subscriptions[subID]; !ok {
				t.Errorf("clientToSubs[%q] has sub %q which is not in subscriptions", clientID, subID)
			}
		}
	}

	for subID, tags := range tr.subToTags {
		if _, ok := tr.subscriptions[subID]; !ok {
			t.Errorf("subToTags has sub %q which is not in subscriptions", subID)
		}
		for tag := range tags {
			if _, ok := tr.tagsToSubs[tag][subID]; !ok {
				t.Errorf("subToTags[%q] has tag %q but tagsToSubs does not map it back", subID, tag)
			}
		}
	}

	for tag, subIDs := range tr.tagsToSubs {
		if len(subIDs) == 0 {
			t.Errorf("tagsToSubs[%q] is an empty map (should have been cleaned up)", tag)
		}
		for subID := range subIDs {
			if _, ok := tr.subscriptions[subID]; !ok {
				t.Errorf("tagsToSubs[%q] has sub %q which is not in subscriptions", tag, subID)
			}
			if _, ok := tr.subToTags[subID][tag]; !ok {
				t.Errorf("tagsToSubs[%q] has sub %q but subToTags does not map it back", tag, subID)
			}
		}
	}
}

func subscriptionForTag(subs []*Subscription, subID string) *Subscription {
	for _, sub := range subs {
		if sub != nil && sub.SubID == subID {
			return sub
		}
	}
	return nil
}

func TestTrackAddsClient(t *testing.T) {
	tr := NewTracker()
	client := NewClient(nil)

	tr.Track(client)

	got, ok := tr.clients[client.ID]
	if !ok {
		t.Fatalf("client %q was not added to tracker", client.ID)
	}
	if got != client {
		t.Errorf("clients[%q] = %p, want the tracked client %p", client.ID, got, client)
	}
	if len(tr.clients) != 1 {
		t.Errorf("len(clients) = %d, want 1", len(tr.clients))
	}
	assertTrackerConsistent(t, tr)
}

func TestTrackMultipleClients(t *testing.T) {
	tr := NewTracker()
	a := NewClient(nil)
	b := NewClient(nil)

	tr.Track(a)
	tr.Track(b)

	if len(tr.clients) != 2 {
		t.Errorf("len(clients) = %d, want 2", len(tr.clients))
	}
	if tr.clients[a.ID] != a {
		t.Errorf("clients[%q] is not client a", a.ID)
	}
	if tr.clients[b.ID] != b {
		t.Errorf("clients[%q] is not client b", b.ID)
	}
	assertTrackerConsistent(t, tr)
}

func TestTagMapsToSubscriptionAndClient(t *testing.T) {
	tr := NewTracker()
	client := NewClient(nil)
	tr.Track(client)

	params := map[string]interface{}{"post_id": 42}
	sub := tr.SubscribeToQuery(client.ID, "getPost", "post:42", params)
	if sub == nil {
		t.Fatal("SubscribeToQuery returned nil")
	}
	if sub.Client != client {
		t.Errorf("subscription.Client = %p, want tracked client %p", sub.Client, client)
	}
	if sub.Query != "getPost" {
		t.Errorf("subscription.Query = %q, want %q", sub.Query, "getPost")
	}
	if sub.QueryKey != "post:42" {
		t.Errorf("subscription.QueryKey = %q, want %q", sub.QueryKey, "post:42")
	}

	tr.UpdateTags(sub.SubID, []string{"post:42", "user:7"})

	for _, tag := range []string{"post:42", "user:7"} {
		subs := tr.GetSubscriptionsToTag(tag)
		got := subscriptionForTag(subs, sub.SubID)
		if got == nil {
			t.Fatalf("GetSubscriptionsToTag(%q) did not return subscription %q", tag, sub.SubID)
		}
		if got != sub {
			t.Errorf("GetSubscriptionsToTag(%q) returned a different subscription object", tag)
		}
		if got.Client != client {
			t.Errorf("GetSubscriptionsToTag(%q) subscription.Client = %p, want %p", tag, got.Client, client)
		}
		if got.Client.ID != client.ID {
			t.Errorf("GetSubscriptionsToTag(%q) client ID = %q, want %q", tag, got.Client.ID, client.ID)
		}
	}

	unrelated := tr.GetSubscriptionsToTag("post:999")
	if len(unrelated) != 0 {
		t.Errorf("GetSubscriptionsToTag(unrelated) = %d subs, want 0", len(unrelated))
	}

	assertTrackerConsistent(t, tr)
}

func TestSameTagMapsToMultipleClients(t *testing.T) {
	tr := NewTracker()
	a := NewClient(nil)
	b := NewClient(nil)
	tr.Track(a)
	tr.Track(b)

	subA := tr.SubscribeToQuery(a.ID, "feed", "feed-a", map[string]interface{}{"n": 1})
	subB := tr.SubscribeToQuery(b.ID, "feed", "feed-b", map[string]interface{}{"n": 2})
	tr.UpdateTags(subA.SubID, []string{"feed"})
	tr.UpdateTags(subB.SubID, []string{"feed"})

	subs := tr.GetSubscriptionsToTag("feed")
	if len(subs) != 2 {
		t.Fatalf("GetSubscriptionsToTag(feed) = %d subs, want 2", len(subs))
	}

	gotA := subscriptionForTag(subs, subA.SubID)
	gotB := subscriptionForTag(subs, subB.SubID)
	if gotA == nil || gotA.Client != a {
		t.Errorf("missing subscription for client a")
	}
	if gotB == nil || gotB.Client != b {
		t.Errorf("missing subscription for client b")
	}
	assertTrackerConsistent(t, tr)
}

func TestUpdateTagsReplacesNonPermanentTags(t *testing.T) {
	tr := NewTracker()
	client := NewClient(nil)
	tr.Track(client)

	sub := tr.SubscribeToQuery(client.ID, "q", "k", nil)
	tr.UpdateTags(sub.SubID, []string{"old", "*keep"})
	tr.UpdateTags(sub.SubID, []string{"new"})

	if got := tr.GetSubscriptionsToTag("old"); subscriptionForTag(got, sub.SubID) != nil {
		t.Error("subscription still mapped to replaced tag \"old\"")
	}
	if got := tr.GetSubscriptionsToTag("new"); subscriptionForTag(got, sub.SubID) == nil {
		t.Error("subscription not mapped to new tag")
	}
	if got := tr.GetSubscriptionsToTag("*keep"); subscriptionForTag(got, sub.SubID) == nil {
		t.Error("subscription lost permanent tag \"*keep\"")
	}
	assertTrackerConsistent(t, tr)
}

func TestUntrackRemovesClientFromAllMaps(t *testing.T) {
	tr := NewTracker()
	client := NewClient(nil)
	tr.Track(client)

	params1 := map[string]interface{}{"id": 1}
	params2 := map[string]interface{}{"id": 2}
	sub1 := tr.SubscribeToQuery(client.ID, "getPost", "k1", params1)
	sub2 := tr.SubscribeToQuery(client.ID, "getComments", "k2", params2)
	tr.UpdateTags(sub1.SubID, []string{"post:1", "user:1"})
	tr.UpdateTags(sub2.SubID, []string{"post:1", "comments:1"})

	sub1ID, sub2ID := sub1.SubID, sub2.SubID
	clientID := client.ID

	tr.Untrack(client)

	if _, ok := tr.clients[clientID]; ok {
		t.Error("client still in clients after Untrack")
	}
	if _, ok := tr.clientToSubs[clientID]; ok {
		t.Error("client still in clientToSubs after Untrack")
	}
	if _, ok := tr.subscriptions[sub1ID]; ok {
		t.Error("sub1 still in subscriptions after Untrack")
	}
	if _, ok := tr.subscriptions[sub2ID]; ok {
		t.Error("sub2 still in subscriptions after Untrack")
	}
	if _, ok := tr.subToTags[sub1ID]; ok {
		t.Error("sub1 still in subToTags after Untrack")
	}
	if _, ok := tr.subToTags[sub2ID]; ok {
		t.Error("sub2 still in subToTags after Untrack")
	}
	for _, tag := range []string{"post:1", "user:1", "comments:1"} {
		if _, ok := tr.tagsToSubs[tag]; ok {
			t.Errorf("tag %q still in tagsToSubs after Untrack", tag)
		}
		if subs := tr.GetSubscriptionsToTag(tag); len(subs) != 0 {
			t.Errorf("GetSubscriptionsToTag(%q) = %d subs after Untrack, want 0", tag, len(subs))
		}
	}

	assertTrackerEmpty(t, tr)
	assertTrackerConsistent(t, tr)
}

func TestUntrackDoesNotRemoveOtherClients(t *testing.T) {
	tr := NewTracker()
	keep := NewClient(nil)
	drop := NewClient(nil)
	tr.Track(keep)
	tr.Track(drop)

	subKeep := tr.SubscribeToQuery(keep.ID, "feed", "keep", map[string]interface{}{"who": "keep"})
	subDrop := tr.SubscribeToQuery(drop.ID, "feed", "drop", map[string]interface{}{"who": "drop"})
	tr.UpdateTags(subKeep.SubID, []string{"feed", "keep-only"})
	tr.UpdateTags(subDrop.SubID, []string{"feed", "drop-only"})

	tr.Untrack(drop)

	if _, ok := tr.clients[keep.ID]; !ok {
		t.Fatal("kept client was removed")
	}
	if _, ok := tr.subscriptions[subKeep.SubID]; !ok {
		t.Fatal("kept subscription was removed")
	}
	if _, ok := tr.subscriptions[subDrop.SubID]; ok {
		t.Error("dropped subscription still present")
	}

	feedSubs := tr.GetSubscriptionsToTag("feed")
	if subscriptionForTag(feedSubs, subKeep.SubID) == nil {
		t.Error("kept client lost the shared \"feed\" tag")
	}
	if subscriptionForTag(feedSubs, subDrop.SubID) != nil {
		t.Error("dropped client still listed under shared \"feed\" tag")
	}
	if len(tr.GetSubscriptionsToTag("drop-only")) != 0 {
		t.Error("dropped client's exclusive tag still has subscriptions")
	}
	if subscriptionForTag(tr.GetSubscriptionsToTag("keep-only"), subKeep.SubID) == nil {
		t.Error("kept client's exclusive tag mapping was lost")
	}

	assertTrackerConsistent(t, tr)
}

func TestUnsubscribeRemovesOnlyThatSubscription(t *testing.T) {
	tr := NewTracker()
	client := NewClient(nil)
	tr.Track(client)

	paramsA := map[string]interface{}{"id": "a"}
	paramsB := map[string]interface{}{"id": "b"}
	subA := tr.SubscribeToQuery(client.ID, "q", "a", paramsA)
	subB := tr.SubscribeToQuery(client.ID, "q", "b", paramsB)
	tr.UpdateTags(subA.SubID, []string{"tag-a"})
	tr.UpdateTags(subB.SubID, []string{"tag-b"})

	tr.UnsubscribeFromQuery(client.ID, "q", paramsA)

	if _, ok := tr.subscriptions[subA.SubID]; ok {
		t.Error("unsubscribed subscription still present")
	}
	if _, ok := tr.subscriptions[subB.SubID]; !ok {
		t.Error("other subscription was removed")
	}
	if _, ok := tr.clients[client.ID]; !ok {
		t.Error("client was removed by UnsubscribeFromQuery")
	}
	if len(tr.GetSubscriptionsToTag("tag-a")) != 0 {
		t.Error("unsubscribed tag still has subscriptions")
	}
	if subscriptionForTag(tr.GetSubscriptionsToTag("tag-b"), subB.SubID) == nil {
		t.Error("remaining subscription lost its tag")
	}

	assertTrackerConsistent(t, tr)
}

func TestSubscribeWithoutTrackedClientDoesNotPanic(t *testing.T) {
	tr := NewTracker()
	client := NewClient(nil)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("SubscribeToQuery/Untrack panicked when the client was not tracked: %v", r)
		}
	}()

	sub := tr.SubscribeToQuery(client.ID, "q", "k", nil)
	if sub != nil {
		tr.UpdateTags(sub.SubID, []string{"tag"})
	}
	tr.Untrack(client)
}

func TestConcurrentClientLifecycles(t *testing.T) {
	tr := NewTracker()

	const goroutines = 64
	const clientsPerGoroutine = 32

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("goroutine %d panicked: %v", g, r)
				}
			}()
			<-start

			for i := 0; i < clientsPerGoroutine; i++ {
				client := NewClient(nil)
				tr.Track(client)

				params := map[string]interface{}{"g": g, "i": i}
				sub := tr.SubscribeToQuery(client.ID, "items", fmt.Sprintf("k-%d-%d", g, i), params)
				if sub == nil {
					t.Errorf("SubscribeToQuery returned nil for goroutine %d client %d", g, i)
					tr.Untrack(client)
					continue
				}

				tags := []string{
					fmt.Sprintf("g:%d", g),
					fmt.Sprintf("item:%d", i),
					"shared",
				}
				tr.UpdateTags(sub.SubID, tags)

				for _, tag := range tags {
					_ = tr.GetSubscriptionsToTag(tag)
				}

				if i%2 == 0 {
					tr.UnsubscribeFromQuery(client.ID, "items", params)
				}
				tr.Untrack(client)
			}
		}(g)
	}

	close(start)
	wg.Wait()

	assertTrackerEmpty(t, tr)
	assertTrackerConsistent(t, tr)
}

func TestConcurrentSharedClients(t *testing.T) {
	prevLog := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLog) })

	tr := NewTracker()

	const clientCount = 32
	clients := make([]*Client, clientCount)
	for i := range clients {
		clients[i] = NewClient(nil)
		tr.Track(clients[i])
	}

	const goroutines = 64
	const opsPerGoroutine = 64

	var panicCount atomic.Int64
	var firstPanic atomic.Value
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					if panicCount.Add(1) == 1 {
						firstPanic.Store(r)
					}
				}
			}()
			<-start

			for i := 0; i < opsPerGoroutine; i++ {
				client := clients[(g+i)%clientCount]
				query := fmt.Sprintf("q-%d", i%8)
				params := map[string]interface{}{"n": i % 4}

				switch i % 5 {
				case 0:
					tr.Track(client)
				case 1:
					sub := tr.SubscribeToQuery(client.ID, query, "key", params)
					if sub != nil {
						tr.UpdateTags(sub.SubID, []string{
							fmt.Sprintf("tag:%d", i%7),
							"shared",
						})
					}
				case 2:
					_ = tr.GetSubscriptionsToTag("shared")
					_ = tr.GetSubscriptionsToTag(fmt.Sprintf("tag:%d", i%7))
				case 3:
					tr.UnsubscribeFromQuery(client.ID, query, params)
				case 4:
					tr.Untrack(client)
					tr.Track(client)
				}
			}
		}(g)
	}

	close(start)
	wg.Wait()

	nilClients := 0
	for _, sub := range tr.subscriptions {
		if sub == nil || sub.Client == nil {
			nilClients++
		}
	}
	if n := panicCount.Load(); n > 0 {
		t.Errorf("%d goroutines panicked under concurrent Track/Subscribe/Untrack of shared clients; first panic: %v", n, firstPanic.Load())
	}
	if nilClients > 0 {
		t.Errorf("%d leftover subscriptions have a nil Client (SubscribeToQuery after Untrack)", nilClients)
		return
	}

	for _, client := range clients {
		tr.Untrack(client)
	}

	assertTrackerConsistent(t, tr)
	assertTrackerEmpty(t, tr)
}
