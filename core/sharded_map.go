// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 고성능 샤딩된 맵을 구현합니다.
package core

import (
	"hash/fnv"
	"sync"
)

// =============================================================================
// ShardedMap: 샤딩된 Concurrent Map
// =============================================================================
// 단일 뮤텍스 대신 여러 샤드로 분할하여 락 경합을 줄입니다.
// 기본 256개 샤드로 동시성 성능을 극대화합니다.
// =============================================================================

const (
	defaultShardCount = 256
	shardMask         = defaultShardCount - 1 // 비트 연산용 마스크
)

// ShardedMap은 샤딩된 concurrent map입니다.
type ShardedMap[V any] struct {
	shards [defaultShardCount]shard[V]
}

// shard는 개별 샤드입니다.
type shard[V any] struct {
	items map[string]V
	mu    sync.RWMutex
}

// NewShardedMap은 새로운 샤딩된 맵을 생성합니다.
func NewShardedMap[V any]() *ShardedMap[V] {
	sm := &ShardedMap[V]{}
	for i := 0; i < defaultShardCount; i++ {
		sm.shards[i].items = make(map[string]V)
	}
	return sm
}

// getShard는 키에 해당하는 샤드를 반환합니다.
func (m *ShardedMap[V]) getShard(key string) *shard[V] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &m.shards[h.Sum32()&shardMask]
}

// Get은 키에 해당하는 값을 조회합니다.
func (m *ShardedMap[V]) Get(key string) (V, bool) {
	shard := m.getShard(key)
	shard.mu.RLock()
	val, ok := shard.items[key]
	shard.mu.RUnlock()
	return val, ok
}

// Set은 키-값 쌍을 저장합니다.
func (m *ShardedMap[V]) Set(key string, val V) {
	shard := m.getShard(key)
	shard.mu.Lock()
	shard.items[key] = val
	shard.mu.Unlock()
}

// Delete는 키를 삭제합니다.
func (m *ShardedMap[V]) Delete(key string) bool {
	shard := m.getShard(key)
	shard.mu.Lock()
	_, ok := shard.items[key]
	if ok {
		delete(shard.items, key)
	}
	shard.mu.Unlock()
	return ok
}

// Has는 키가 존재하는지 확인합니다.
func (m *ShardedMap[V]) Has(key string) bool {
	shard := m.getShard(key)
	shard.mu.RLock()
	_, ok := shard.items[key]
	shard.mu.RUnlock()
	return ok
}

// Len은 전체 항목 수를 반환합니다.
func (m *ShardedMap[V]) Len() int {
	count := 0
	for i := 0; i < defaultShardCount; i++ {
		m.shards[i].mu.RLock()
		count += len(m.shards[i].items)
		m.shards[i].mu.RUnlock()
	}
	return count
}

// Clear는 모든 항목을 삭제합니다.
func (m *ShardedMap[V]) Clear() {
	for i := 0; i < defaultShardCount; i++ {
		m.shards[i].mu.Lock()
		m.shards[i].items = make(map[string]V)
		m.shards[i].mu.Unlock()
	}
}

// Range는 모든 항목에 대해 함수를 실행합니다.
// 콜백이 false를 반환하면 순회를 중단합니다.
func (m *ShardedMap[V]) Range(f func(key string, val V) bool) {
	for i := 0; i < defaultShardCount; i++ {
		m.shards[i].mu.RLock()
		for k, v := range m.shards[i].items {
			if !f(k, v) {
				m.shards[i].mu.RUnlock()
				return
			}
		}
		m.shards[i].mu.RUnlock()
	}
}

// Keys는 모든 키를 반환합니다.
func (m *ShardedMap[V]) Keys() []string {
	keys := make([]string, 0, m.Len())
	for i := 0; i < defaultShardCount; i++ {
		m.shards[i].mu.RLock()
		for k := range m.shards[i].items {
			keys = append(keys, k)
		}
		m.shards[i].mu.RUnlock()
	}
	return keys
}

// GetMany는 여러 키를 한번에 조회합니다.
// 샤드별로 그룹화하여 락 횟수를 최소화합니다.
func (m *ShardedMap[V]) GetMany(keys []string) map[string]V {
	// 샤드별로 그룹화
	shardKeys := make(map[uint32][]string)
	for _, key := range keys {
		h := fnv.New32a()
		h.Write([]byte(key))
		idx := h.Sum32() & shardMask
		shardKeys[idx] = append(shardKeys[idx], key)
	}

	result := make(map[string]V, len(keys))

	// 샤드별로 조회
	for idx, ks := range shardKeys {
		m.shards[idx].mu.RLock()
		for _, k := range ks {
			if v, ok := m.shards[idx].items[k]; ok {
				result[k] = v
			}
		}
		m.shards[idx].mu.RUnlock()
	}

	return result
}

// SetMany는 여러 키-값 쌍을 한번에 저장합니다.
func (m *ShardedMap[V]) SetMany(items map[string]V) {
	// 샤드별로 그룹화
	shardItems := make(map[uint32]map[string]V)
	for k, v := range items {
		h := fnv.New32a()
		h.Write([]byte(k))
		idx := h.Sum32() & shardMask
		if shardItems[idx] == nil {
			shardItems[idx] = make(map[string]V)
		}
		shardItems[idx][k] = v
	}

	// 샤드별로 저장
	for idx, its := range shardItems {
		m.shards[idx].mu.Lock()
		for k, v := range its {
			m.shards[idx].items[k] = v
		}
		m.shards[idx].mu.Unlock()
	}
}

// DeleteMany는 여러 키를 한번에 삭제합니다.
func (m *ShardedMap[V]) DeleteMany(keys []string) int {
	// 샤드별로 그룹화
	shardKeys := make(map[uint32][]string)
	for _, key := range keys {
		h := fnv.New32a()
		h.Write([]byte(key))
		idx := h.Sum32() & shardMask
		shardKeys[idx] = append(shardKeys[idx], key)
	}

	count := 0

	// 샤드별로 삭제
	for idx, ks := range shardKeys {
		m.shards[idx].mu.Lock()
		for _, k := range ks {
			if _, ok := m.shards[idx].items[k]; ok {
				delete(m.shards[idx].items, k)
				count++
			}
		}
		m.shards[idx].mu.Unlock()
	}

	return count
}
