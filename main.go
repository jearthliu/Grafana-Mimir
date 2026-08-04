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
// then verifies the epoch hasn't changed before writing the result to the
// cache — if a rebalance happened mid-query, the cache write is discarded so
// stale fragments never land under the new topology's keys.
func (f *QueryFrontend) handleQuery(tenantID string, reqHash uint64, result string) string {
	startEpoch := f.tracker.CurrentEpoch()
	key := cacheKey(f.tracker, tenantID, reqHash)

	// Simulate query execution. If a rebalance occurred mid-flight (epoch
	// changed), discard the write — the caller should retry rather than cache
	// a result built from a stale shard assignment.
	if f.tracker.CurrentEpoch() != startEpoch {
		return "" // signal: not cached, retry required
	}

	f.mu.Lock()
	f.cache[key] = result
	f.mu.Unlock()
	return key
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

	fmt.Println("Simulation passed: cache keys diverge across shard epochs.")
}
