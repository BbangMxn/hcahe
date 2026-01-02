// Package memory는 인메모리 캐시 어댑터를 구현합니다.
package memory

import (
	"container/list"
	"sync"
)

// =============================================================================
// LRU: Least Recently Used 캐시
// =============================================================================
// LRU는 가장 오래 사용되지 않은 항목을 먼저 제거하는 캐시입니다.
// O(1) 시간 복잡도로 Get/Set/Delete를 수행합니다.
//
// 구현 방식:
// - HashMap: O(1) 키 조회
// - DoublyLinkedList: O(1) 순서 조정 (가장 최근 사용 항목을 앞으로)
// =============================================================================

// lruItem은 LRU 리스트의 항목입니다.
type lruItem[K comparable, V any] struct {
	key   K
	value V
}

// LRU는 LRU 캐시입니다.
type LRU[K comparable, V any] struct {
	// capacity는 최대 항목 수입니다.
	capacity int

	// items는 키-리스트요소 매핑입니다.
	items map[K]*list.Element

	// order는 사용 순서를 유지하는 리스트입니다.
	// Front = 가장 최근 사용, Back = 가장 오래 사용 안 됨
	order *list.List

	// mu는 동시 접근을 위한 RWMutex입니다.
	mu sync.RWMutex

	// onEvict는 항목이 제거될 때 호출되는 콜백입니다.
	onEvict func(key K, value V)
}

// =============================================================================
// LRU 생성자
// =============================================================================

// NewLRU는 새로운 LRU 캐시를 생성합니다.
//
// Parameters:
//   - capacity: 최대 항목 수 (0 이하면 제한 없음)
//
// Returns:
//   - *LRU: 생성된 LRU 캐시
func NewLRU[K comparable, V any](capacity int) *LRU[K, V] {
	return &LRU[K, V]{
		capacity: capacity,
		items:    make(map[K]*list.Element),
		order:    list.New(),
	}
}

// =============================================================================
// LRU 옵션
// =============================================================================

// SetOnEvict는 퇴거 콜백을 설정합니다.
func (l *LRU[K, V]) SetOnEvict(fn func(key K, value V)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onEvict = fn
}

// SetCapacity는 용량을 변경합니다.
// 새 용량이 현재 크기보다 작으면 초과 항목이 제거됩니다.
func (l *LRU[K, V]) SetCapacity(capacity int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.capacity = capacity

	// 용량 초과 시 제거
	for l.capacity > 0 && l.order.Len() > l.capacity {
		l.evictOldest()
	}
}

// =============================================================================
// LRU CRUD 연산
// =============================================================================

// Get은 키에 해당하는 값을 조회합니다.
// 조회 시 해당 항목을 가장 최근으로 이동합니다.
//
// Returns:
//   - value: 값 (없으면 zero value)
//   - ok: 존재 여부
func (l *LRU[K, V]) Get(key K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	elem, exists := l.items[key]
	if !exists {
		var zero V
		return zero, false
	}

	// 가장 최근으로 이동
	l.order.MoveToFront(elem)

	item := elem.Value.(*lruItem[K, V])
	return item.value, true
}

// Peek은 순서를 변경하지 않고 값을 조회합니다.
func (l *LRU[K, V]) Peek(key K) (V, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	elem, exists := l.items[key]
	if !exists {
		var zero V
		return zero, false
	}

	item := elem.Value.(*lruItem[K, V])
	return item.value, true
}

// Set은 키-값을 저장합니다.
// 용량 초과 시 가장 오래된 항목이 제거됩니다.
//
// Returns:
//   - evicted: 제거된 항목이 있는지 여부
func (l *LRU[K, V]) Set(key K, value V) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 이미 존재하면 업데이트
	if elem, exists := l.items[key]; exists {
		l.order.MoveToFront(elem)
		item := elem.Value.(*lruItem[K, V])
		item.value = value
		return false
	}

	// 용량 초과 시 제거
	evicted := false
	if l.capacity > 0 && l.order.Len() >= l.capacity {
		l.evictOldest()
		evicted = true
	}

	// 새 항목 추가
	item := &lruItem[K, V]{key: key, value: value}
	elem := l.order.PushFront(item)
	l.items[key] = elem

	return evicted
}

// Delete는 키를 삭제합니다.
//
// Returns:
//   - ok: 삭제 여부 (존재하지 않으면 false)
func (l *LRU[K, V]) Delete(key K) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	elem, exists := l.items[key]
	if !exists {
		return false
	}

	l.removeElement(elem)
	return true
}

// Has는 키가 존재하는지 확인합니다 (순서 변경 없음).
func (l *LRU[K, V]) Has(key K) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	_, exists := l.items[key]
	return exists
}

// =============================================================================
// LRU 관리
// =============================================================================

// Len은 현재 저장된 항목 수를 반환합니다.
func (l *LRU[K, V]) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.order.Len()
}

// Capacity는 최대 용량을 반환합니다.
func (l *LRU[K, V]) Capacity() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.capacity
}

// Clear는 모든 항목을 삭제합니다.
func (l *LRU[K, V]) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 콜백 호출
	if l.onEvict != nil {
		for _, elem := range l.items {
			item := elem.Value.(*lruItem[K, V])
			l.onEvict(item.key, item.value)
		}
	}

	l.items = make(map[K]*list.Element)
	l.order.Init()
}

// Keys는 모든 키를 반환합니다 (가장 최근 사용 순).
func (l *LRU[K, V]) Keys() []K {
	l.mu.RLock()
	defer l.mu.RUnlock()

	keys := make([]K, 0, l.order.Len())
	for elem := l.order.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*lruItem[K, V])
		keys = append(keys, item.key)
	}
	return keys
}

// Values는 모든 값을 반환합니다 (가장 최근 사용 순).
func (l *LRU[K, V]) Values() []V {
	l.mu.RLock()
	defer l.mu.RUnlock()

	values := make([]V, 0, l.order.Len())
	for elem := l.order.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*lruItem[K, V])
		values = append(values, item.value)
	}
	return values
}

// Oldest는 가장 오래된 항목을 반환합니다 (제거하지 않음).
func (l *LRU[K, V]) Oldest() (K, V, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	elem := l.order.Back()
	if elem == nil {
		var zeroK K
		var zeroV V
		return zeroK, zeroV, false
	}

	item := elem.Value.(*lruItem[K, V])
	return item.key, item.value, true
}

// Newest는 가장 최근 항목을 반환합니다 (제거하지 않음).
func (l *LRU[K, V]) Newest() (K, V, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	elem := l.order.Front()
	if elem == nil {
		var zeroK K
		var zeroV V
		return zeroK, zeroV, false
	}

	item := elem.Value.(*lruItem[K, V])
	return item.key, item.value, true
}

// =============================================================================
// LRU 내부 헬퍼
// =============================================================================

// evictOldest는 가장 오래된 항목을 제거합니다.
// mu.Lock() 상태에서 호출되어야 합니다.
func (l *LRU[K, V]) evictOldest() {
	elem := l.order.Back()
	if elem == nil {
		return
	}
	l.removeElement(elem)
}

// removeElement는 요소를 제거합니다.
// mu.Lock() 상태에서 호출되어야 합니다.
func (l *LRU[K, V]) removeElement(elem *list.Element) {
	item := elem.Value.(*lruItem[K, V])

	// 콜백 호출
	if l.onEvict != nil {
		l.onEvict(item.key, item.value)
	}

	// 맵에서 제거
	delete(l.items, item.key)

	// 리스트에서 제거
	l.order.Remove(elem)
}

// =============================================================================
// LRU 순회
// =============================================================================

// Range는 모든 항목에 대해 함수를 실행합니다.
// 함수가 false를 반환하면 순회를 중단합니다.
// 가장 최근 사용 순으로 순회합니다.
func (l *LRU[K, V]) Range(fn func(key K, value V) bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for elem := l.order.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*lruItem[K, V])
		if !fn(item.key, item.value) {
			return
		}
	}
}

// RangeReverse는 역순으로 순회합니다.
// 가장 오래된 순으로 순회합니다.
func (l *LRU[K, V]) RangeReverse(fn func(key K, value V) bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for elem := l.order.Back(); elem != nil; elem = elem.Prev() {
		item := elem.Value.(*lruItem[K, V])
		if !fn(item.key, item.value) {
			return
		}
	}
}
