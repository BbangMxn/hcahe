// Package memory는 인메모리 캐시 어댑터를 구현합니다.
// 가장 빠른 L1 캐시로 사용됩니다.
package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bridgify/hcache/core"
)

// =============================================================================
// Adapter: 고성능 인메모리 캐시 어댑터
// =============================================================================
// 샤딩된 LRU를 사용하여 락 경합을 최소화합니다.
//
// 성능 특성:
// - Get: ~100ns (샤드 락만 사용)
// - Set: ~200ns (샤드 락만 사용)
// - 동시성: 256개 샤드로 높은 병렬 처리량
// =============================================================================

// Config는 메모리 어댑터 설정입니다.
type Config struct {
	// MaxSize는 최대 저장 항목 수입니다.
	MaxSize int

	// CleanupInterval은 만료된 항목 정리 주기입니다.
	// 0이면 자동 정리 비활성화
	CleanupInterval time.Duration
}

// DefaultConfig는 기본 설정을 반환합니다.
func DefaultConfig() *Config {
	return &Config{
		MaxSize:         10000,
		CleanupInterval: time.Minute,
	}
}

// Adapter는 고성능 인메모리 캐시 어댑터입니다.
type Adapter struct {
	config *Config
	cache  *ShardedLRU

	// 추가 메트릭
	getLatency int64
	getCount   uint64
	setLatency int64
	setCount   uint64

	// cleanup
	cleanupTicker *time.Ticker
	closeCh       chan struct{}
	closed        bool
	mu            sync.RWMutex
}

// New는 새로운 메모리 어댑터를 생성합니다.
func New(config *Config) *Adapter {
	if config == nil {
		config = DefaultConfig()
	}
	if config.MaxSize <= 0 {
		config.MaxSize = 10000
	}

	return &Adapter{
		config:  config,
		cache:   NewShardedLRU(config.MaxSize),
		closeCh: make(chan struct{}),
	}
}

// =============================================================================
// Adapter 인터페이스 구현 - 기본 정보
// =============================================================================

func (a *Adapter) Name() string           { return "memory" }
func (a *Adapter) Type() core.AdapterType { return core.TypeMemory }

// =============================================================================
// Adapter 인터페이스 구현 - 연결 관리
// =============================================================================

func (a *Adapter) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		a.closed = false
		a.closeCh = make(chan struct{})
	}

	// 자동 정리 시작
	if a.config.CleanupInterval > 0 && a.cleanupTicker == nil {
		a.cleanupTicker = time.NewTicker(a.config.CleanupInterval)
		go a.cleanupLoop()
	}

	return nil
}

func (a *Adapter) Disconnect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}

	a.closed = true
	close(a.closeCh)

	if a.cleanupTicker != nil {
		a.cleanupTicker.Stop()
		a.cleanupTicker = nil
	}

	return nil
}

func (a *Adapter) IsConnected() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return !a.closed
}

func (a *Adapter) Ping(ctx context.Context) error {
	return nil
}

// =============================================================================
// Adapter 인터페이스 구현 - CRUD
// =============================================================================

func (a *Adapter) Get(ctx context.Context, key string) (*core.Entry, error) {
	start := time.Now()

	entry, found := a.cache.Get(key)

	// 메트릭 업데이트
	atomic.AddInt64(&a.getLatency, time.Since(start).Nanoseconds())
	atomic.AddUint64(&a.getCount, 1)

	if !found {
		return nil, nil
	}
	return entry, nil
}

func (a *Adapter) Set(ctx context.Context, entry *core.Entry) error {
	start := time.Now()

	a.cache.Set(entry)

	atomic.AddInt64(&a.setLatency, time.Since(start).Nanoseconds())
	atomic.AddUint64(&a.setCount, 1)

	return nil
}

func (a *Adapter) Delete(ctx context.Context, key string) (bool, error) {
	return a.cache.Delete(key), nil
}

func (a *Adapter) Has(ctx context.Context, key string) (bool, error) {
	return a.cache.Has(key), nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 배치 연산
// =============================================================================

func (a *Adapter) GetMany(ctx context.Context, keys []string) (map[string]*core.Entry, error) {
	return a.cache.GetMany(keys), nil
}

func (a *Adapter) SetMany(ctx context.Context, entries []*core.Entry) error {
	a.cache.SetMany(entries)
	return nil
}

func (a *Adapter) DeleteMany(ctx context.Context, keys []string) (int, error) {
	count := 0
	for _, key := range keys {
		if a.cache.Delete(key) {
			count++
		}
	}
	return count, nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 관리
// =============================================================================

func (a *Adapter) Keys(ctx context.Context, pattern string) ([]string, error) {
	allKeys := a.cache.Keys()

	if pattern == "" || pattern == "*" {
		return allKeys, nil
	}

	// 간단한 prefix 매칭
	result := make([]string, 0, len(allKeys)/2)
	for _, key := range allKeys {
		if matchSimplePattern(pattern, key) {
			result = append(result, key)
		}
	}
	return result, nil
}

func (a *Adapter) Clear(ctx context.Context) error {
	a.cache.Clear()
	return nil
}

func (a *Adapter) Size(ctx context.Context) (int64, error) {
	return int64(a.cache.Len()), nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 메트릭
// =============================================================================

func (a *Adapter) Stats(ctx context.Context) (*core.AdapterStats, error) {
	hits, misses, evictions := a.cache.Stats()
	getCount := atomic.LoadUint64(&a.getCount)
	setCount := atomic.LoadUint64(&a.setCount)

	var avgGetLatency, avgSetLatency int64
	if getCount > 0 {
		avgGetLatency = atomic.LoadInt64(&a.getLatency) / int64(getCount)
	}
	if setCount > 0 {
		avgSetLatency = atomic.LoadInt64(&a.setLatency) / int64(setCount)
	}

	return &core.AdapterStats{
		Name:         a.Name(),
		Type:         a.Type(),
		Connected:    a.IsConnected(),
		Size:         int64(a.cache.Len()),
		MaxSize:      int64(a.config.MaxSize),
		Hits:         hits,
		Misses:       misses,
		Evictions:    evictions,
		GetLatencyNs: avgGetLatency,
		SetLatencyNs: avgSetLatency,
	}, nil
}

// =============================================================================
// 내부 헬퍼
// =============================================================================

func (a *Adapter) cleanupLoop() {
	for {
		select {
		case <-a.cleanupTicker.C:
			a.cache.CleanupExpired()
		case <-a.closeCh:
			return
		}
	}
}

// matchSimplePattern은 간단한 패턴 매칭을 수행합니다.
func matchSimplePattern(pattern, str string) bool {
	// prefix* 형태
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(str) >= len(prefix) && str[:len(prefix)] == prefix
	}
	// *suffix 형태
	if len(pattern) > 0 && pattern[0] == '*' {
		suffix := pattern[1:]
		return len(str) >= len(suffix) && str[len(str)-len(suffix):] == suffix
	}
	// 정확히 일치
	return pattern == str
}
