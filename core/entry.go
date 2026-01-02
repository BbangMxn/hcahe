// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 캐시 엔트리(저장 단위)를 정의합니다.
package core

import (
	"sync"
	"time"
)

// =============================================================================
// Entry: 캐시에 저장되는 단위 데이터
// =============================================================================
// Entry는 실제 데이터와 메타데이터를 함께 저장합니다.
// 메타데이터는 TTL, 접근 횟수, 생성 시간 등을 포함하여
// 자동 티어링과 만료 처리에 사용됩니다.
// =============================================================================

// Entry는 캐시에 저장되는 개별 항목을 나타냅니다.
type Entry struct {
	// Key는 캐시 키입니다.
	Key string `json:"key" msgpack:"key"`

	// Value는 직렬화된 실제 데이터입니다.
	// 원본 타입 정보는 보존되지 않으므로 역직렬화 시 타입을 지정해야 합니다.
	Value []byte `json:"value" msgpack:"value"`

	// CreatedAt은 엔트리가 생성된 시간입니다.
	CreatedAt time.Time `json:"created_at" msgpack:"created_at"`

	// ExpiresAt은 엔트리가 만료되는 시간입니다.
	// Zero value인 경우 만료되지 않습니다.
	ExpiresAt time.Time `json:"expires_at" msgpack:"expires_at"`

	// AccessCount는 이 엔트리가 조회된 횟수입니다.
	// 자동 티어링(승격/강등) 결정에 사용됩니다.
	AccessCount uint64 `json:"access_count" msgpack:"access_count"`

	// LastAccessedAt은 마지막으로 조회된 시간입니다.
	// LRU 정책과 강등 결정에 사용됩니다.
	LastAccessedAt time.Time `json:"last_accessed_at" msgpack:"last_accessed_at"`

	// Size는 Value의 바이트 크기입니다.
	// 메모리 사용량 계산에 사용됩니다.
	Size int `json:"size" msgpack:"size"`

	// Compressed는 Value가 압축되었는지 여부입니다.
	Compressed bool `json:"compressed" msgpack:"compressed"`

	// CompressionType은 사용된 압축 알고리즘입니다.
	// 예: "gzip", "lz4", "zstd", "none"
	CompressionType string `json:"compression_type,omitempty" msgpack:"compression_type,omitempty"`

	// mu는 동시 접근을 위한 뮤텍스입니다.
	mu sync.RWMutex `json:"-" msgpack:"-"`
}

// =============================================================================
// Entry 생성자
// =============================================================================

// NewEntry는 새로운 캐시 엔트리를 생성합니다.
//
// Parameters:
//   - key: 캐시 키
//   - value: 직렬화된 데이터
//   - ttl: 만료 시간 (0이면 만료 없음)
//
// Returns:
//   - *Entry: 생성된 엔트리
func NewEntry(key string, value []byte, ttl time.Duration) *Entry {
	now := time.Now()

	entry := &Entry{
		Key:            key,
		Value:          value,
		CreatedAt:      now,
		AccessCount:    0,
		LastAccessedAt: now,
		Size:           len(value),
		Compressed:     false,
	}

	// TTL이 0보다 크면 만료 시간 설정
	if ttl > 0 {
		entry.ExpiresAt = now.Add(ttl)
	}

	return entry
}

// NewEntryPooled는 풀에서 엔트리를 가져와 초기화합니다.
// 고성능이 필요한 경우 사용합니다.
// 사용 후 ReleaseEntry로 반환해야 합니다.
func NewEntryPooled(key string, value []byte, ttl time.Duration) *Entry {
	entry := globalEntryPool.Get()
	now := time.Now()

	entry.Key = key
	entry.Value = value
	entry.CreatedAt = now
	entry.AccessCount = 0
	entry.LastAccessedAt = now
	entry.Size = len(value)
	entry.Compressed = false
	entry.CompressionType = ""

	if ttl > 0 {
		entry.ExpiresAt = now.Add(ttl)
	} else {
		entry.ExpiresAt = time.Time{}
	}

	return entry
}

// ReleaseEntry는 엔트리를 풀에 반환합니다.
func ReleaseEntry(e *Entry) {
	globalEntryPool.Put(e)
}

// =============================================================================
// Entry 메서드
// =============================================================================

// IsExpired는 엔트리가 만료되었는지 확인합니다.
// 락-프리 버전으로 최적화되었습니다.
//
// Returns:
//   - bool: 만료 여부
func (e *Entry) IsExpired() bool {
	// ExpiresAt이 Zero value면 만료되지 않음
	if e.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(e.ExpiresAt)
}

// IsExpiredAt은 주어진 시간 기준으로 만료 여부를 확인합니다.
// time.Now() 호출을 줄이기 위해 사용합니다.
func (e *Entry) IsExpiredAt(now time.Time) bool {
	if e.ExpiresAt.IsZero() {
		return false
	}
	return now.After(e.ExpiresAt)
}

// Touch는 엔트리에 접근했음을 기록합니다.
// AccessCount를 증가시키고 LastAccessedAt을 현재 시간으로 갱신합니다.
func (e *Entry) Touch() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.AccessCount++
	e.LastAccessedAt = time.Now()
}

// Age는 엔트리가 생성된 후 경과한 시간을 반환합니다.
//
// Returns:
//   - time.Duration: 경과 시간
func (e *Entry) Age() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return time.Since(e.CreatedAt)
}

// TTL은 남은 만료 시간을 반환합니다.
//
// Returns:
//   - time.Duration: 남은 시간 (만료 없으면 -1, 이미 만료되었으면 0)
func (e *Entry) TTL() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.ExpiresAt.IsZero() {
		return -1 // 만료 없음
	}

	remaining := time.Until(e.ExpiresAt)
	if remaining < 0 {
		return 0 // 이미 만료됨
	}

	return remaining
}

// IdleTime은 마지막 접근 후 경과한 시간을 반환합니다.
// 강등 결정에 사용됩니다.
//
// Returns:
//   - time.Duration: 유휴 시간
func (e *Entry) IdleTime() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return time.Since(e.LastAccessedAt)
}

// Clone은 엔트리의 복사본을 생성합니다.
// 동시성 문제를 피하기 위해 깊은 복사를 수행합니다.
//
// Returns:
//   - *Entry: 복사된 엔트리
func (e *Entry) Clone() *Entry {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Value 복사
	valueCopy := make([]byte, len(e.Value))
	copy(valueCopy, e.Value)

	return &Entry{
		Key:             e.Key,
		Value:           valueCopy,
		CreatedAt:       e.CreatedAt,
		ExpiresAt:       e.ExpiresAt,
		AccessCount:     e.AccessCount,
		LastAccessedAt:  e.LastAccessedAt,
		Size:            e.Size,
		Compressed:      e.Compressed,
		CompressionType: e.CompressionType,
	}
}

// UpdateValue는 엔트리의 값을 업데이트합니다.
//
// Parameters:
//   - value: 새로운 직렬화된 데이터
//   - ttl: 새로운 TTL (0이면 기존 TTL 유지, -1이면 만료 없음)
func (e *Entry) UpdateValue(value []byte, ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.Value = value
	e.Size = len(value)

	if ttl > 0 {
		e.ExpiresAt = time.Now().Add(ttl)
	} else if ttl == -1 {
		e.ExpiresAt = time.Time{} // 만료 없음
	}
	// ttl == 0이면 기존 ExpiresAt 유지
}

// =============================================================================
// Entry 메트릭
// =============================================================================

// Metrics는 엔트리의 메트릭 정보를 반환합니다.
type EntryMetrics struct {
	Key         string        `json:"key"`
	Size        int           `json:"size"`
	Age         time.Duration `json:"age"`
	TTL         time.Duration `json:"ttl"`
	IdleTime    time.Duration `json:"idle_time"`
	AccessCount uint64        `json:"access_count"`
	Compressed  bool          `json:"compressed"`
}

// Metrics는 엔트리의 메트릭 정보를 반환합니다.
func (e *Entry) Metrics() EntryMetrics {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return EntryMetrics{
		Key:         e.Key,
		Size:        e.Size,
		Age:         time.Since(e.CreatedAt),
		TTL:         e.TTL(),
		IdleTime:    time.Since(e.LastAccessedAt),
		AccessCount: e.AccessCount,
		Compressed:  e.Compressed,
	}
}
