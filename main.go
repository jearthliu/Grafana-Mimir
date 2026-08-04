package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// ShardTracker tracks the active shard assignment epoch for a tenant.
// Routing updates are atomic and thread-safe: reads use atomic.LoadUint64 so
// concurrent query handling never observes a torn epoch value.
type ShardTracker struct {
	// epoch is the current shard-assignment epoch. Any change to a tenant's
	// shard topology bumps it, rotating cache keys so fragments from an old
	// assignment can never merge with new ones.
	epoch uint64
}

// newShardTracker returns a tracker starting at epoch 1.
func newShardTracker() *ShardTracker {
	return &ShardTracker{epoch: 1}
}

// CurrentEpoch returns the active shard epoch (atomic read).
func (t *ShardTracker) CurrentEpoch() uint64 {
	return atomic.LoadUint64(&t.epoch)
}

// Rebalance simulates a tenant shard-assignment change, bumping the epoch.
func (t *ShardTracker) Rebalance() {
	atomic.AddUint64(&t.epoch, 1)
}

// cacheKey builds a topology-aware cache key. Including the shard epoch means
// the key naturally rotates on rebalance, so cached query fragments generated
// under an old shard assignment are never merged with fragments from the new
// assignment. The query hash is part of the key as before.
func cacheKey(t *ShardTracker, tenantID string, reqHash uint64) string {
	epoch := t.CurrentEpoch()
	return fmt.Sprintf("%s:%d:%d", tenantID, epoch, reqHash)
}

// QueryFrontend holds the shard tracker and simulates query handling with
// cache validation.
type QueryFrontend struct {
	tracker *ShardTracker
	mu      sync.Mutex
	cache   map[string]string
}

func newQueryFrontend() *QueryFrontend {
	return &QueryFrontend{
		tracker: newShardTracker(),
		cache:   make(map[string]string),
	}
}

// handleQuery simulates a PromQL query. It captures the shard epoch at start,
// then atomically verifies the epoch hasn't changed and writes the result to
// the cache. The check-and-write happen under the same lock, so a rebalance
// that lands between the check and the write cannot slip a stale fragment in.
// If a rebalance happened mid-query, the cache write is discarded and "" is
// returned, signalling the caller to retry.
func (f *QueryFrontend) handleQuery(tenantID string, reqHash uint64, result string) string {
	startEpoch := f.tracker.CurrentEpoch()
	key := cacheKey(f.tracker, tenantID, reqHash)

	f.mu.Lock()
	defer f.mu.Unlock()

	// Simulate query execution finishing, then re-verify the epoch under the
	// lock before committing the cache write.
	if f.tracker.CurrentEpoch() != startEpoch {
		return "" // not cached; caller should retry
	}
	f.cache[key] = result
	return key
}

// cachedResult reads a query result from the cache for the current epoch.
// Keys rotate with the epoch, so a read here can never return a fragment
// produced under a different shard assignment.
func (f *QueryFrontend) cachedResult(tenantID string, reqHash uint64) (string, bool) {
	key := cacheKey(f.tracker, tenantID, reqHash)
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.cache[key]
	return v, ok
}

func main() {
	fmt.Println("Running Query Frontend Shard Rebalance Simulation...")

	f := newQueryFrontend()

	// Query under epoch 1
	key1 := f.handleQuery("tenant-a", 12345, "result-v1")
	fmt.Println("Cached under:", key1)

	// Rebalance -> epoch 2
	f.tracker.Rebalance()

	// Same query now produces a different cache key (no collision with old fragment)
	key2 := f.handleQuery("tenant-a", 12345, "result-v2")
	fmt.Println("Cached under:", key2)

	if key1 == key2 {
		panic("cache key collision: same key across different shard epochs")
	}

	// Read path: the old epoch's fragment must NOT be readable under the new
	// epoch (keys rotated), and the new fragment must be.
	if v, ok := f.cachedResult("tenant-a", 12345); !ok || v != "result-v2" {
		panic(fmt.Sprintf("expected result-v2 under current epoch, got %q ok=%v", v, ok))
	}

	fmt.Println("Simulation passed: cache keys diverge across shard epochs, reads are epoch-consistent.")
}
