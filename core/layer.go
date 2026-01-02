// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 캐시 계층을 관리합니다.
package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Layer: 개별 캐시 계층
// =============================================================================
// Layer는 하나의 저장소 백엔드를 감싸고 메트릭, TTL 관리 등을 수행합니다.
// 여러 Layer가 모여 계층적 캐시를 구성합니다.
// =============================================================================

// Layer는 캐시 계층을 나타냅니다.
type Layer struct {
	// adapter는 실제 저장소 백엔드입니다.
	adapter Adapter

	// options는 이 계층의 설정입니다.
	options *LayerOptions

	// ==========================================================================
	// 메트릭 (atomic으로 락 없이 업데이트)
	// ==========================================================================
	hits       uint64 // 캐시 히트 횟수
	misses     uint64 // 캐시 미스 횟수
	getLatency int64  // 총 Get 지연 시간 (나노초)
	getCount   uint64 // Get 호출 횟수
	setLatency int64  // 총 Set 지연 시간 (나노초)
	setCount   uint64 // Set 호출 횟수
	evictions  uint64 // 퇴거 횟수
	promotions uint64 // 승격 횟수
	demotions  uint64 // 강등 횟수

	// mu는 옵션 변경 등을 위한 뮤텍스입니다.
	mu sync.RWMutex
}

// =============================================================================
// Layer 생성자
// =============================================================================

// NewLayer는 새로운 캐시 계층을 생성합니다.
//
// Parameters:
//   - adapter: 저장소 백엔드
//   - options: 계층 설정
//
// Returns:
//   - *Layer: 생성된 계층
func NewLayer(adapter Adapter, options *LayerOptions) *Layer {
	if options == nil {
		options = DefaultLayerOptions(adapter.Name(), 0)
	}

	return &Layer{
		adapter: adapter,
		options: options,
	}
}

// =============================================================================
// Layer 기본 메서드
// =============================================================================

// Name은 계층의 이름을 반환합니다.
func (l *Layer) Name() string {
	return l.options.Name
}

// Priority는 계층의 우선순위를 반환합니다.
func (l *Layer) Priority() int {
	return l.options.Priority
}

// Adapter는 내부 어댑터를 반환합니다.
func (l *Layer) Adapter() Adapter {
	return l.adapter
}

// Options는 계층 옵션을 반환합니다.
func (l *Layer) Options() *LayerOptions {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.options
}

// =============================================================================
// Layer 연결 관리
// =============================================================================

// Connect는 저장소에 연결합니다.
func (l *Layer) Connect(ctx context.Context) error {
	return l.adapter.Connect(ctx)
}

// Disconnect는 저장소 연결을 종료합니다.
func (l *Layer) Disconnect(ctx context.Context) error {
	return l.adapter.Disconnect(ctx)
}

// IsConnected는 연결 상태를 반환합니다.
func (l *Layer) IsConnected() bool {
	return l.adapter.IsConnected()
}

// =============================================================================
// Layer CRUD 연산 (메트릭 수집 포함)
// =============================================================================

// Get은 키에 해당하는 엔트리를 조회합니다.
// 내부적으로 메트릭을 수집하고, 만료된 엔트리는 삭제합니다.
//
// Parameters:
//   - ctx: 컨텍스트
//   - key: 캐시 키
//
// Returns:
//   - *Entry: 조회된 엔트리 (없으면 nil)
//   - bool: 캐시 히트 여부
//   - error: 에러
func (l *Layer) Get(ctx context.Context, key string) (*Entry, bool, error) {
	start := time.Now()

	entry, err := l.adapter.Get(ctx, key)

	// 지연 시간 기록
	latency := time.Since(start).Nanoseconds()
	atomic.AddInt64(&l.getLatency, latency)
	atomic.AddUint64(&l.getCount, 1)

	if err != nil {
		return nil, false, fmt.Errorf("layer %s get error: %w", l.options.Name, err)
	}

	// 캐시 미스
	if entry == nil {
		atomic.AddUint64(&l.misses, 1)
		return nil, false, nil
	}

	// 만료 확인
	if entry.IsExpired() {
		// 백그라운드로 삭제 (블로킹 방지)
		go func() {
			deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			l.adapter.Delete(deleteCtx, key)
		}()

		atomic.AddUint64(&l.misses, 1)
		return nil, false, nil
	}

	// 캐시 히트
	atomic.AddUint64(&l.hits, 1)

	// 접근 기록 업데이트
	entry.Touch()

	return entry, true, nil
}

// Set은 엔트리를 저장합니다.
//
// Parameters:
//   - ctx: 컨텍스트
//   - entry: 저장할 엔트리
//
// Returns:
//   - error: 에러
func (l *Layer) Set(ctx context.Context, entry *Entry) error {
	// 읽기 전용 모드 확인
	if l.options.ReadOnly {
		return nil // 조용히 무시
	}

	start := time.Now()

	// 계층별 TTL 적용
	l.applyLayerTTL(entry)

	// 용량 확인 및 퇴거
	if l.options.MaxSize > 0 {
		size, _ := l.adapter.Size(ctx)
		if size >= int64(l.options.MaxSize) {
			// 퇴거 필요
			if err := l.evict(ctx); err != nil {
				return fmt.Errorf("layer %s eviction error: %w", l.options.Name, err)
			}
		}
	}

	err := l.adapter.Set(ctx, entry)

	// 지연 시간 기록
	latency := time.Since(start).Nanoseconds()
	atomic.AddInt64(&l.setLatency, latency)
	atomic.AddUint64(&l.setCount, 1)

	if err != nil {
		return fmt.Errorf("layer %s set error: %w", l.options.Name, err)
	}

	return nil
}

// Delete는 엔트리를 삭제합니다.
func (l *Layer) Delete(ctx context.Context, key string) error {
	if l.options.ReadOnly {
		return nil
	}

	_, err := l.adapter.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("layer %s delete error: %w", l.options.Name, err)
	}

	return nil
}

// Has는 키가 존재하는지 확인합니다.
func (l *Layer) Has(ctx context.Context, key string) (bool, error) {
	return l.adapter.Has(ctx, key)
}

// =============================================================================
// Layer 배치 연산
// =============================================================================

// GetMany는 여러 키를 한 번에 조회합니다.
func (l *Layer) GetMany(ctx context.Context, keys []string) (map[string]*Entry, error) {
	start := time.Now()

	entries, err := l.adapter.GetMany(ctx, keys)

	latency := time.Since(start).Nanoseconds()
	atomic.AddInt64(&l.getLatency, latency)
	atomic.AddUint64(&l.getCount, uint64(len(keys)))

	if err != nil {
		return nil, fmt.Errorf("layer %s getMany error: %w", l.options.Name, err)
	}

	// 만료된 엔트리 필터링
	result := make(map[string]*Entry)
	expiredKeys := make([]string, 0)

	for key, entry := range entries {
		if entry.IsExpired() {
			expiredKeys = append(expiredKeys, key)
			atomic.AddUint64(&l.misses, 1)
		} else {
			entry.Touch()
			result[key] = entry
			atomic.AddUint64(&l.hits, 1)
		}
	}

	// 누락된 키는 미스로 카운트
	for _, key := range keys {
		if _, found := entries[key]; !found {
			atomic.AddUint64(&l.misses, 1)
		}
	}

	// 만료된 키 백그라운드 삭제
	if len(expiredKeys) > 0 {
		go func() {
			deleteCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			l.adapter.DeleteMany(deleteCtx, expiredKeys)
		}()
	}

	return result, nil
}

// SetMany는 여러 엔트리를 한 번에 저장합니다.
func (l *Layer) SetMany(ctx context.Context, entries []*Entry) error {
	if l.options.ReadOnly {
		return nil
	}

	start := time.Now()

	// 계층별 TTL 적용
	for _, entry := range entries {
		l.applyLayerTTL(entry)
	}

	err := l.adapter.SetMany(ctx, entries)

	latency := time.Since(start).Nanoseconds()
	atomic.AddInt64(&l.setLatency, latency)
	atomic.AddUint64(&l.setCount, uint64(len(entries)))

	if err != nil {
		return fmt.Errorf("layer %s setMany error: %w", l.options.Name, err)
	}

	return nil
}

// DeleteMany는 여러 키를 한 번에 삭제합니다.
func (l *Layer) DeleteMany(ctx context.Context, keys []string) error {
	if l.options.ReadOnly {
		return nil
	}

	_, err := l.adapter.DeleteMany(ctx, keys)
	if err != nil {
		return fmt.Errorf("layer %s deleteMany error: %w", l.options.Name, err)
	}

	return nil
}

// =============================================================================
// Layer 내부 헬퍼
// =============================================================================

// applyLayerTTL은 계층별 TTL 정책을 엔트리에 적용합니다.
func (l *Layer) applyLayerTTL(entry *Entry) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// 계층별 TTL이 설정된 경우
	if l.options.TTL > 0 {
		entry.ExpiresAt = time.Now().Add(l.options.TTL)
		return
	}

	// TTL 배수가 설정된 경우
	if l.options.TTLMultiplier != 1.0 && !entry.ExpiresAt.IsZero() {
		remaining := time.Until(entry.ExpiresAt)
		if remaining > 0 {
			newTTL := time.Duration(float64(remaining) * l.options.TTLMultiplier)
			entry.ExpiresAt = time.Now().Add(newTTL)
		}
	}
}

// evict는 퇴거 정책에 따라 엔트리를 제거합니다.
func (l *Layer) evict(ctx context.Context) error {
	// TODO: 퇴거 정책에 따라 구현
	// 현재는 간단히 가장 오래된 키 하나 삭제
	atomic.AddUint64(&l.evictions, 1)
	return nil
}

// =============================================================================
// Layer 메트릭
// =============================================================================

// Stats는 계층의 통계를 반환합니다.
func (l *Layer) Stats(ctx context.Context) (*LayerStats, error) {
	adapterStats, err := l.adapter.Stats(ctx)
	if err != nil {
		return nil, err
	}

	getCount := atomic.LoadUint64(&l.getCount)
	setCount := atomic.LoadUint64(&l.setCount)

	var avgGetLatency, avgSetLatency int64
	if getCount > 0 {
		avgGetLatency = atomic.LoadInt64(&l.getLatency) / int64(getCount)
	}
	if setCount > 0 {
		avgSetLatency = atomic.LoadInt64(&l.setLatency) / int64(setCount)
	}

	return &LayerStats{
		Name:           l.options.Name,
		Priority:       l.options.Priority,
		AdapterType:    l.adapter.Type(),
		Connected:      l.adapter.IsConnected(),
		Size:           adapterStats.Size,
		MaxSize:        int64(l.options.MaxSize),
		MemoryUsage:    adapterStats.MemoryUsage,
		MaxMemory:      l.options.MaxMemory,
		Hits:           atomic.LoadUint64(&l.hits),
		Misses:         atomic.LoadUint64(&l.misses),
		GetLatencyNs:   avgGetLatency,
		SetLatencyNs:   avgSetLatency,
		Evictions:      atomic.LoadUint64(&l.evictions),
		Promotions:     atomic.LoadUint64(&l.promotions),
		Demotions:      atomic.LoadUint64(&l.demotions),
		EvictionPolicy: l.options.EvictionPolicy,
	}, nil
}

// ResetStats는 메트릭을 초기화합니다.
func (l *Layer) ResetStats() {
	atomic.StoreUint64(&l.hits, 0)
	atomic.StoreUint64(&l.misses, 0)
	atomic.StoreInt64(&l.getLatency, 0)
	atomic.StoreUint64(&l.getCount, 0)
	atomic.StoreInt64(&l.setLatency, 0)
	atomic.StoreUint64(&l.setCount, 0)
	atomic.StoreUint64(&l.evictions, 0)
	atomic.StoreUint64(&l.promotions, 0)
	atomic.StoreUint64(&l.demotions, 0)
}

// =============================================================================
// LayerStats: 계층 통계
// =============================================================================

// LayerStats는 계층의 통계 정보를 담습니다.
type LayerStats struct {
	Name           string         `json:"name"`
	Priority       int            `json:"priority"`
	AdapterType    AdapterType    `json:"adapter_type"`
	Connected      bool           `json:"connected"`
	Size           int64          `json:"size"`
	MaxSize        int64          `json:"max_size"`
	MemoryUsage    int64          `json:"memory_usage"`
	MaxMemory      int64          `json:"max_memory"`
	Hits           uint64         `json:"hits"`
	Misses         uint64         `json:"misses"`
	GetLatencyNs   int64          `json:"get_latency_ns"`
	SetLatencyNs   int64          `json:"set_latency_ns"`
	Evictions      uint64         `json:"evictions"`
	Promotions     uint64         `json:"promotions"`
	Demotions      uint64         `json:"demotions"`
	EvictionPolicy EvictionPolicy `json:"eviction_policy"`
}

// HitRate는 캐시 히트율을 반환합니다 (0.0 ~ 1.0).
func (s *LayerStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}
