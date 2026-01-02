// Package memory는 인메모리 캐시 어댑터를 구현합니다.
// 이 파일은 고성능 샤딩된 LRU 캐시를 구현합니다.
package memory

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BbangMxn/hcahe/core"
)

// =============================================================================
// ShardedLRU: 샤딩된 LRU 캐시
// =============================================================================
// 256개 샤드로 분할하여 락 경합을 최소화합니다.
// 각 샤드는 독립적인 LRU 캐시입니다.
// =============================================================================

const (
	shardCount = 256
	shardMask  = shardCount - 1
)

// ShardedLRU는 샤딩된 LRU 캐시입니다.
type ShardedLRU struct {
	shards   [shardCount]*lruShard
	maxSize  int // 전체 최대 크기
	shardMax int // 샤드당 최대 크기

	// 전역 통계 (atomic)
	hits      uint64
	misses    uint64
	evictions uint64
}

// lruShard는 개별 LRU 샤드입니다.
type lruShard struct {
	items    map[string]*list.Element
	evictLst *list.List
	maxSize  int
	mu       sync.RWMutex
}

// shardedLruItem은 ShardedLRU 리스트의 항목입니다.
type shardedLruItem struct {
	key   string
	entry *core.Entry
}

// NewShardedLRU는 새로운 샤딩된 LRU를 생성합니다.
func NewShardedLRU(maxSize int) *ShardedLRU {
	shardMax := maxSize / shardCount
	if shardMax < 1 {
		shardMax = 1
	}

	lru := &ShardedLRU{
		maxSize:  maxSize,
		shardMax: shardMax,
	}

	for i := 0; i < shardCount; i++ {
		lru.shards[i] = &lruShard{
			items:    make(map[string]*list.Element),
			evictLst: list.New(),
			maxSize:  shardMax,
		}
	}

	return lru
}

// getShard는 키에 해당하는 샤드를 반환합니다.
// FNV-1a 해시를 인라인으로 계산하여 오버헤드 최소화
func (l *ShardedLRU) getShard(key string) *lruShard {
	h := uint32(2166136261) // FNV offset basis
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619 // FNV prime
	}
	return l.shards[h&shardMask]
}

// Get은 키에 해당하는 엔트리를 조회합니다.
func (l *ShardedLRU) Get(key string) (*core.Entry, bool) {
	shard := l.getShard(key)

	shard.mu.Lock()
	elem, ok := shard.items[key]
	if !ok {
		shard.mu.Unlock()
		atomic.AddUint64(&l.misses, 1)
		return nil, false
	}

	item := elem.Value.(*shardedLruItem)
	entry := item.entry

	// 만료 체크 - 인라인으로 최적화
	if !entry.ExpiresAt.IsZero() && time.Now().After(entry.ExpiresAt) {
		shard.evictLst.Remove(elem)
		delete(shard.items, key)
		shard.mu.Unlock()
		atomic.AddUint64(&l.misses, 1)
		return nil, false
	}

	// LRU 업데이트 - 맨 앞으로 이동
	shard.evictLst.MoveToFront(elem)
	shard.mu.Unlock()

	// 락 외부에서 atomic 연산 (락 범위 최소화)
	atomic.AddUint64(&entry.AccessCount, 1)
	atomic.AddUint64(&l.hits, 1)

	return entry, true
}

// Set은 엔트리를 저장합니다.
func (l *ShardedLRU) Set(entry *core.Entry) {
	if entry == nil || entry.Key == "" {
		return
	}

	key := entry.Key
	shard := l.getShard(key)

	shard.mu.Lock()

	// 이미 존재하면 업데이트
	if elem, ok := shard.items[key]; ok {
		shard.evictLst.MoveToFront(elem)
		elem.Value.(*shardedLruItem).entry = entry
		shard.mu.Unlock()
		return
	}

	// 용량 초과시 퇴거
	for len(shard.items) >= shard.maxSize {
		l.evictOldest(shard)
	}

	// 새 항목 추가
	item := &shardedLruItem{key: key, entry: entry}
	elem := shard.evictLst.PushFront(item)
	shard.items[key] = elem
	shard.mu.Unlock()
}

// Delete는 키를 삭제합니다.
func (l *ShardedLRU) Delete(key string) bool {
	shard := l.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	elem, ok := shard.items[key]
	if !ok {
		return false
	}

	shard.evictLst.Remove(elem)
	delete(shard.items, key)
	return true
}

// Has는 키가 존재하는지 확인합니다.
func (l *ShardedLRU) Has(key string) bool {
	shard := l.getShard(key)

	shard.mu.RLock()
	_, ok := shard.items[key]
	shard.mu.RUnlock()

	return ok
}

// Peek은 LRU 순서를 변경하지 않고 조회합니다.
func (l *ShardedLRU) Peek(key string) (*core.Entry, bool) {
	shard := l.getShard(key)

	shard.mu.RLock()
	elem, ok := shard.items[key]
	if !ok {
		shard.mu.RUnlock()
		return nil, false
	}

	entry := elem.Value.(*shardedLruItem).entry
	shard.mu.RUnlock()

	if entry.IsExpired() {
		return nil, false
	}

	return entry, true
}

// Len은 전체 항목 수를 반환합니다.
func (l *ShardedLRU) Len() int {
	count := 0
	for i := 0; i < shardCount; i++ {
		l.shards[i].mu.RLock()
		count += len(l.shards[i].items)
		l.shards[i].mu.RUnlock()
	}
	return count
}

// Clear는 모든 항목을 삭제합니다.
func (l *ShardedLRU) Clear() {
	for i := 0; i < shardCount; i++ {
		l.shards[i].mu.Lock()
		l.shards[i].items = make(map[string]*list.Element)
		l.shards[i].evictLst.Init()
		l.shards[i].mu.Unlock()
	}
}

// evictOldest는 가장 오래된 항목을 퇴거합니다.
// 호출자가 락을 보유해야 합니다.
func (l *ShardedLRU) evictOldest(shard *lruShard) {
	elem := shard.evictLst.Back()
	if elem == nil {
		return
	}

	item := elem.Value.(*shardedLruItem)
	shard.evictLst.Remove(elem)
	delete(shard.items, item.key)
	atomic.AddUint64(&l.evictions, 1)
}

// Keys는 모든 키를 반환합니다.
func (l *ShardedLRU) Keys() []string {
	keys := make([]string, 0, l.Len())
	for i := 0; i < shardCount; i++ {
		l.shards[i].mu.RLock()
		for k := range l.shards[i].items {
			keys = append(keys, k)
		}
		l.shards[i].mu.RUnlock()
	}
	return keys
}

// Range는 모든 항목에 대해 함수를 실행합니다.
func (l *ShardedLRU) Range(f func(key string, entry *core.Entry) bool) {
	for i := 0; i < shardCount; i++ {
		l.shards[i].mu.RLock()
		for _, elem := range l.shards[i].items {
			item := elem.Value.(*shardedLruItem)
			if !f(item.key, item.entry) {
				l.shards[i].mu.RUnlock()
				return
			}
		}
		l.shards[i].mu.RUnlock()
	}
}

// Stats는 통계를 반환합니다.
func (l *ShardedLRU) Stats() (hits, misses, evictions uint64) {
	return atomic.LoadUint64(&l.hits),
		atomic.LoadUint64(&l.misses),
		atomic.LoadUint64(&l.evictions)
}

// GetMany는 여러 키를 한번에 조회합니다.
func (l *ShardedLRU) GetMany(keys []string) map[string]*core.Entry {
	result := make(map[string]*core.Entry, len(keys))

	// 샤드별로 그룹화
	shardKeys := make(map[int][]string)
	for _, key := range keys {
		h := uint32(2166136261)
		for i := 0; i < len(key); i++ {
			h ^= uint32(key[i])
			h *= 16777619
		}
		idx := int(h & shardMask)
		shardKeys[idx] = append(shardKeys[idx], key)
	}

	// 샤드별로 조회
	for idx, ks := range shardKeys {
		shard := l.shards[idx]
		shard.mu.Lock()
		for _, key := range ks {
			if elem, ok := shard.items[key]; ok {
				item := elem.Value.(*shardedLruItem)
				if !item.entry.IsExpired() {
					shard.evictLst.MoveToFront(elem)
					result[key] = item.entry
					atomic.AddUint64(&l.hits, 1)
				} else {
					shard.evictLst.Remove(elem)
					delete(shard.items, key)
					atomic.AddUint64(&l.misses, 1)
				}
			} else {
				atomic.AddUint64(&l.misses, 1)
			}
		}
		shard.mu.Unlock()
	}

	return result
}

// SetMany는 여러 엔트리를 한번에 저장합니다.
func (l *ShardedLRU) SetMany(entries []*core.Entry) {
	// 샤드별로 그룹화
	shardEntries := make(map[int][]*core.Entry)
	for _, entry := range entries {
		if entry == nil || entry.Key == "" {
			continue
		}
		h := uint32(2166136261)
		for i := 0; i < len(entry.Key); i++ {
			h ^= uint32(entry.Key[i])
			h *= 16777619
		}
		idx := int(h & shardMask)
		shardEntries[idx] = append(shardEntries[idx], entry)
	}

	// 샤드별로 저장
	for idx, ents := range shardEntries {
		shard := l.shards[idx]
		shard.mu.Lock()
		for _, entry := range ents {
			if elem, ok := shard.items[entry.Key]; ok {
				shard.evictLst.MoveToFront(elem)
				elem.Value.(*shardedLruItem).entry = entry
			} else {
				for len(shard.items) >= shard.maxSize {
					l.evictOldest(shard)
				}
				item := &shardedLruItem{key: entry.Key, entry: entry}
				elem := shard.evictLst.PushFront(item)
				shard.items[entry.Key] = elem
			}
		}
		shard.mu.Unlock()
	}
}

// CleanupExpired는 만료된 항목을 정리합니다.
func (l *ShardedLRU) CleanupExpired() int {
	count := 0
	now := time.Now()

	for i := 0; i < shardCount; i++ {
		shard := l.shards[i]
		shard.mu.Lock()

		// 뒤에서부터 순회하며 만료된 항목 제거
		var next *list.Element
		for elem := shard.evictLst.Back(); elem != nil; elem = next {
			next = elem.Prev()
			item := elem.Value.(*shardedLruItem)
			if !item.entry.ExpiresAt.IsZero() && item.entry.ExpiresAt.Before(now) {
				shard.evictLst.Remove(elem)
				delete(shard.items, item.key)
				count++
			}
		}

		shard.mu.Unlock()
	}

	return count
}
