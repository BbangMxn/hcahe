// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Graceful Degradation을 구현합니다.
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Graceful Degradation: 장애 시 stale 데이터 반환
// =============================================================================
// 백엔드 장애 시에도 만료된(stale) 데이터를 반환하여
// 서비스 가용성을 유지합니다.
//
// 전략:
// - Stale-While-Revalidate: 만료된 데이터 반환 + 백그라운드 갱신
// - Stale-If-Error: 에러 시에만 만료된 데이터 반환
// - Fallback: 지정된 기본값 반환
// =============================================================================

// DegradationMode는 Degradation 모드입니다.
type DegradationMode int

const (
	// DegradationNone은 Degradation을 사용하지 않습니다.
	DegradationNone DegradationMode = iota

	// DegradationStaleWhileRevalidate는 만료된 데이터를 반환하면서 백그라운드에서 갱신합니다.
	DegradationStaleWhileRevalidate

	// DegradationStaleIfError는 에러 발생 시에만 만료된 데이터를 반환합니다.
	DegradationStaleIfError
)

// StaleEntry는 만료된 데이터를 저장하는 구조체입니다.
type StaleEntry struct {
	Entry      *Entry
	ExpiredAt  time.Time
	StaleUntil time.Time // 이 시간까지 stale 데이터로 사용 가능
}

// StaleCache는 만료된 데이터를 보관하는 캐시입니다.
type StaleCache struct {
	entries sync.Map      // key -> *StaleEntry
	maxAge  time.Duration // stale 데이터 최대 보관 시간
	maxSize int
	size    int64

	// 통계
	staleHits   uint64
	staleMisses uint64
}

// StaleCacheConfig는 StaleCache 설정입니다.
type StaleCacheConfig struct {
	MaxAge  time.Duration // stale 데이터 최대 보관 시간
	MaxSize int           // 최대 보관 개수
}

// DefaultStaleCacheConfig는 기본 설정을 반환합니다.
func DefaultStaleCacheConfig() *StaleCacheConfig {
	return &StaleCacheConfig{
		MaxAge:  time.Hour, // 1시간까지 stale 데이터 보관
		MaxSize: 10000,
	}
}

// NewStaleCache는 새로운 StaleCache를 생성합니다.
func NewStaleCache(config *StaleCacheConfig) *StaleCache {
	if config == nil {
		config = DefaultStaleCacheConfig()
	}

	sc := &StaleCache{
		maxAge:  config.MaxAge,
		maxSize: config.MaxSize,
	}

	// 주기적으로 오래된 stale 데이터 정리
	go sc.cleanupLoop()

	return sc
}

// Put은 만료된 엔트리를 stale 캐시에 저장합니다.
func (sc *StaleCache) Put(entry *Entry) {
	if entry == nil {
		return
	}

	// 크기 제한 확인
	if int(atomic.LoadInt64(&sc.size)) >= sc.maxSize {
		return
	}

	stale := &StaleEntry{
		Entry:      entry.Clone(),
		ExpiredAt:  time.Now(),
		StaleUntil: time.Now().Add(sc.maxAge),
	}

	sc.entries.Store(entry.Key, stale)
	atomic.AddInt64(&sc.size, 1)
}

// Get은 stale 엔트리를 조회합니다.
func (sc *StaleCache) Get(key string) (*Entry, bool) {
	val, ok := sc.entries.Load(key)
	if !ok {
		atomic.AddUint64(&sc.staleMisses, 1)
		return nil, false
	}

	stale := val.(*StaleEntry)

	// stale 기간도 지났는지 확인
	if time.Now().After(stale.StaleUntil) {
		sc.entries.Delete(key)
		atomic.AddInt64(&sc.size, -1)
		atomic.AddUint64(&sc.staleMisses, 1)
		return nil, false
	}

	atomic.AddUint64(&sc.staleHits, 1)
	return stale.Entry, true
}

// Delete는 stale 엔트리를 삭제합니다.
func (sc *StaleCache) Delete(key string) {
	if _, ok := sc.entries.LoadAndDelete(key); ok {
		atomic.AddInt64(&sc.size, -1)
	}
}

// Stats는 통계를 반환합니다.
func (sc *StaleCache) Stats() (hits, misses uint64, size int64) {
	return atomic.LoadUint64(&sc.staleHits),
		atomic.LoadUint64(&sc.staleMisses),
		atomic.LoadInt64(&sc.size)
}

func (sc *StaleCache) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		sc.entries.Range(func(key, value interface{}) bool {
			stale := value.(*StaleEntry)
			if now.After(stale.StaleUntil) {
				sc.entries.Delete(key)
				atomic.AddInt64(&sc.size, -1)
			}
			return true
		})
	}
}

// =============================================================================
// GracefulCache: Degradation이 적용된 캐시
// =============================================================================

// GracefulCache는 Graceful Degradation이 적용된 캐시입니다.
type GracefulCache struct {
	cache      *Cache
	staleCache *StaleCache
	mode       DegradationMode
	loader     LoaderFunc // 데이터 로더 (옵션)

	// Stale-While-Revalidate용
	revalidating sync.Map // key -> bool (현재 갱신 중인지)

	// 통계
	degradedResponses uint64
	revalidations     uint64
}

// LoaderFunc는 캐시 미스 시 데이터를 로드하는 함수입니다.
type LoaderFunc func(ctx context.Context, key string) ([]byte, time.Duration, error)

// GracefulCacheConfig는 GracefulCache 설정입니다.
type GracefulCacheConfig struct {
	Mode        DegradationMode
	StaleConfig *StaleCacheConfig
	Loader      LoaderFunc
}

// NewGracefulCache는 새로운 GracefulCache를 생성합니다.
func NewGracefulCache(cache *Cache, config *GracefulCacheConfig) *GracefulCache {
	if config == nil {
		config = &GracefulCacheConfig{
			Mode: DegradationStaleIfError,
		}
	}

	return &GracefulCache{
		cache:      cache,
		staleCache: NewStaleCache(config.StaleConfig),
		mode:       config.Mode,
		loader:     config.Loader,
	}
}

// Get은 캐시를 조회하고, 필요시 stale 데이터를 반환합니다.
func (gc *GracefulCache) Get(ctx context.Context, key string, dest interface{}) (int, bool, error) {
	// 1. 정상 캐시 조회
	layer, err := gc.cache.Get(ctx, key, dest)

	if err == nil && layer >= 0 {
		// 캐시 히트 - 정상 반환
		return layer, false, nil
	}

	// 2. 캐시 미스 또는 에러 - Degradation 모드에 따라 처리
	switch gc.mode {
	case DegradationStaleIfError:
		return gc.handleStaleIfError(ctx, key, dest, err)

	case DegradationStaleWhileRevalidate:
		return gc.handleStaleWhileRevalidate(ctx, key, dest)

	default:
		return layer, false, err
	}
}

func (gc *GracefulCache) handleStaleIfError(ctx context.Context, key string, dest interface{}, originalErr error) (int, bool, error) {
	// Loader가 있으면 먼저 시도
	if gc.loader != nil {
		data, ttl, err := gc.loader(ctx, key)
		if err == nil {
			// 로드 성공 - 캐시에 저장
			gc.cache.Set(ctx, key, data, WithTTL(ttl))

			// dest에 데이터 설정 (바이트 슬라이스인 경우)
			if destBytes, ok := dest.(*[]byte); ok {
				*destBytes = data
			}
			return 0, false, nil
		}
	}

	// Loader 실패 또는 없음 - stale 데이터 확인
	staleEntry, ok := gc.staleCache.Get(key)
	if ok {
		// stale 데이터 반환
		if destBytes, ok := dest.(*[]byte); ok {
			*destBytes = staleEntry.Value
		}
		atomic.AddUint64(&gc.degradedResponses, 1)
		return -1, true, nil // layer=-1, stale=true
	}

	return -1, false, originalErr
}

func (gc *GracefulCache) handleStaleWhileRevalidate(ctx context.Context, key string, dest interface{}) (int, bool, error) {
	// stale 데이터 확인
	staleEntry, ok := gc.staleCache.Get(key)
	if !ok {
		// stale 데이터도 없음 - 동기 로드
		if gc.loader != nil {
			data, ttl, err := gc.loader(ctx, key)
			if err == nil {
				gc.cache.Set(ctx, key, data, WithTTL(ttl))
				if destBytes, ok := dest.(*[]byte); ok {
					*destBytes = data
				}
				return 0, false, nil
			}
			return -1, false, err
		}
		return -1, false, nil
	}

	// stale 데이터 반환
	if destBytes, ok := dest.(*[]byte); ok {
		*destBytes = staleEntry.Value
	}
	atomic.AddUint64(&gc.degradedResponses, 1)

	// 백그라운드 갱신 (중복 방지)
	if gc.loader != nil {
		if _, revalidating := gc.revalidating.LoadOrStore(key, true); !revalidating {
			go gc.revalidate(key)
		}
	}

	return -1, true, nil
}

func (gc *GracefulCache) revalidate(key string) {
	defer gc.revalidating.Delete(key)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	data, ttl, err := gc.loader(ctx, key)
	if err != nil {
		return
	}

	gc.cache.Set(ctx, key, data, WithTTL(ttl))
	gc.staleCache.Delete(key)
	atomic.AddUint64(&gc.revalidations, 1)
}

// Set은 캐시에 저장합니다.
func (gc *GracefulCache) Set(ctx context.Context, key string, value interface{}, opts ...SetOption) error {
	return gc.cache.Set(ctx, key, value, opts...)
}

// OnExpire는 엔트리 만료 시 호출되어 stale 캐시에 저장합니다.
// 이 메서드를 캐시의 만료 콜백으로 등록하세요.
func (gc *GracefulCache) OnExpire(entry *Entry) {
	gc.staleCache.Put(entry)
}

// Stats는 통계를 반환합니다.
func (gc *GracefulCache) Stats() (degraded, revalidations uint64) {
	return atomic.LoadUint64(&gc.degradedResponses),
		atomic.LoadUint64(&gc.revalidations)
}

// Cache는 내부 캐시를 반환합니다.
func (gc *GracefulCache) Cache() *Cache {
	return gc.cache
}

// =============================================================================
// Fallback: 기본값 반환
// =============================================================================

// FallbackCache는 캐시 미스 시 기본값을 반환합니다.
type FallbackCache struct {
	cache     *Cache
	fallbacks sync.Map // key pattern -> fallback value
}

// NewFallbackCache는 새로운 FallbackCache를 생성합니다.
func NewFallbackCache(cache *Cache) *FallbackCache {
	return &FallbackCache{
		cache: cache,
	}
}

// RegisterFallback은 키 패턴에 대한 기본값을 등록합니다.
func (fc *FallbackCache) RegisterFallback(keyPattern string, value []byte) {
	fc.fallbacks.Store(keyPattern, value)
}

// Get은 캐시를 조회하고, 미스 시 등록된 기본값을 반환합니다.
func (fc *FallbackCache) Get(ctx context.Context, key string, dest interface{}) (int, error) {
	layer, err := fc.cache.Get(ctx, key, dest)
	if layer >= 0 {
		return layer, nil
	}

	// 기본값 확인
	if val, ok := fc.fallbacks.Load(key); ok {
		if destBytes, ok := dest.(*[]byte); ok {
			*destBytes = val.([]byte)
		}
		return -1, nil
	}

	return layer, err
}
