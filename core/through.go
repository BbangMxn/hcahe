// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Read-Through / Write-Through 패턴을 구현합니다.
package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Read-Through / Write-Through: DB 자동 연동
// =============================================================================
// 캐시 미스 시 자동으로 DB에서 로드하고,
// 쓰기 시 DB와 캐시를 함께 업데이트합니다.
//
// 패턴:
// - Read-Through: 캐시 미스 → DB 조회 → 캐시 저장 → 반환
// - Write-Through: 캐시 저장 + DB 저장 (동기)
// - Write-Behind: 캐시 저장 → DB 저장 (비동기)
// =============================================================================

// Repository는 데이터 저장소 인터페이스입니다.
type Repository interface {
	// Get은 키에 해당하는 데이터를 조회합니다.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set은 데이터를 저장합니다.
	Set(ctx context.Context, key string, value []byte) error

	// Delete는 데이터를 삭제합니다.
	Delete(ctx context.Context, key string) error

	// GetMany는 여러 키를 한번에 조회합니다.
	GetMany(ctx context.Context, keys []string) (map[string][]byte, error)

	// SetMany는 여러 데이터를 한번에 저장합니다.
	SetMany(ctx context.Context, items map[string][]byte) error
}

// =============================================================================
// ReadThroughCache: Read-Through 캐시
// =============================================================================

// ReadThroughCache는 Read-Through 패턴을 구현합니다.
type ReadThroughCache struct {
	cache *Cache
	repo  Repository
	ttl   time.Duration

	// Singleflight - 동일 키 중복 요청 방지
	inflight sync.Map // key -> *inflightCall

	// 통계
	cacheHits   uint64
	cacheMisses uint64
	repoLoads   uint64
	repoErrors  uint64
}

type inflightCall struct {
	done   chan struct{}
	result []byte
	err    error
}

// ReadThroughConfig는 설정입니다.
type ReadThroughConfig struct {
	TTL time.Duration
}

// NewReadThroughCache는 새로운 Read-Through 캐시를 생성합니다.
func NewReadThroughCache(cache *Cache, repo Repository, config *ReadThroughConfig) *ReadThroughCache {
	ttl := 10 * time.Minute
	if config != nil && config.TTL > 0 {
		ttl = config.TTL
	}

	return &ReadThroughCache{
		cache: cache,
		repo:  repo,
		ttl:   ttl,
	}
}

// Get은 캐시를 조회하고, 미스 시 Repository에서 로드합니다.
func (rtc *ReadThroughCache) Get(ctx context.Context, key string) ([]byte, error) {
	// 1. 캐시 조회
	var data []byte
	layer, err := rtc.cache.Get(ctx, key, &data)
	if err != nil {
		return nil, err
	}

	if layer >= 0 {
		atomic.AddUint64(&rtc.cacheHits, 1)
		return data, nil
	}

	atomic.AddUint64(&rtc.cacheMisses, 1)

	// 2. Singleflight - 동일 키 요청 합치기
	call := &inflightCall{done: make(chan struct{})}
	if existing, loaded := rtc.inflight.LoadOrStore(key, call); loaded {
		// 이미 다른 고루틴이 로딩 중
		existingCall := existing.(*inflightCall)
		select {
		case <-existingCall.done:
			return existingCall.result, existingCall.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// 첫 번째 요청 - Repository에서 로드
	defer rtc.inflight.Delete(key)
	defer close(call.done)

	data, err = rtc.repo.Get(ctx, key)
	if err != nil {
		atomic.AddUint64(&rtc.repoErrors, 1)
		call.err = err
		return nil, err
	}

	atomic.AddUint64(&rtc.repoLoads, 1)
	call.result = data

	// 3. 캐시에 저장
	if data != nil {
		rtc.cache.Set(ctx, key, data, WithTTL(rtc.ttl))
	}

	return data, nil
}

// GetMany는 여러 키를 한번에 조회합니다.
func (rtc *ReadThroughCache) GetMany(ctx context.Context, keys []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(keys))
	var missingKeys []string

	// 1. 캐시에서 먼저 조회
	for _, key := range keys {
		var data []byte
		layer, _ := rtc.cache.Get(ctx, key, &data)
		if layer >= 0 {
			result[key] = data
			atomic.AddUint64(&rtc.cacheHits, 1)
		} else {
			missingKeys = append(missingKeys, key)
			atomic.AddUint64(&rtc.cacheMisses, 1)
		}
	}

	// 2. 미스된 키들은 Repository에서 로드
	if len(missingKeys) > 0 {
		repoData, err := rtc.repo.GetMany(ctx, missingKeys)
		if err != nil {
			atomic.AddUint64(&rtc.repoErrors, 1)
			return result, err
		}

		for key, data := range repoData {
			result[key] = data
			atomic.AddUint64(&rtc.repoLoads, 1)

			// 캐시에 저장
			rtc.cache.Set(ctx, key, data, WithTTL(rtc.ttl))
		}
	}

	return result, nil
}

// Invalidate는 캐시와 Repository 모두에서 삭제합니다.
func (rtc *ReadThroughCache) Invalidate(ctx context.Context, key string) error {
	rtc.cache.Delete(ctx, key)
	return rtc.repo.Delete(ctx, key)
}

// Stats는 통계를 반환합니다.
func (rtc *ReadThroughCache) Stats() (hits, misses, loads, errors uint64) {
	return atomic.LoadUint64(&rtc.cacheHits),
		atomic.LoadUint64(&rtc.cacheMisses),
		atomic.LoadUint64(&rtc.repoLoads),
		atomic.LoadUint64(&rtc.repoErrors)
}

// =============================================================================
// WriteThroughCache: Write-Through 캐시
// =============================================================================

// WriteThroughCache는 Write-Through 패턴을 구현합니다.
type WriteThroughCache struct {
	*ReadThroughCache
	writeMode WriteThroughMode
}

// WriteThroughMode는 쓰기 모드입니다.
type WriteThroughMode int

const (
	// WriteModeSync는 동기 쓰기입니다. (Write-Through)
	WriteModeSync WriteThroughMode = iota

	// WriteModeAsync는 비동기 쓰기입니다. (Write-Behind)
	WriteModeAsync
)

// WriteThroughConfig는 설정입니다.
type WriteThroughConfig struct {
	ReadThroughConfig
	WriteMode WriteThroughMode
}

// NewWriteThroughCache는 새로운 Write-Through 캐시를 생성합니다.
func NewWriteThroughCache(cache *Cache, repo Repository, config *WriteThroughConfig) *WriteThroughCache {
	if config == nil {
		config = &WriteThroughConfig{}
	}

	return &WriteThroughCache{
		ReadThroughCache: NewReadThroughCache(cache, repo, &config.ReadThroughConfig),
		writeMode:        config.WriteMode,
	}
}

// Set은 캐시와 Repository에 동시에 저장합니다.
func (wtc *WriteThroughCache) Set(ctx context.Context, key string, value []byte) error {
	switch wtc.writeMode {
	case WriteModeSync:
		return wtc.writeSync(ctx, key, value)
	case WriteModeAsync:
		return wtc.writeAsync(ctx, key, value)
	default:
		return wtc.writeSync(ctx, key, value)
	}
}

func (wtc *WriteThroughCache) writeSync(ctx context.Context, key string, value []byte) error {
	// Repository에 먼저 저장 (더 안전)
	if err := wtc.repo.Set(ctx, key, value); err != nil {
		return fmt.Errorf("repository write failed: %w", err)
	}

	// 캐시에 저장
	if err := wtc.cache.Set(ctx, key, value, WithTTL(wtc.ttl)); err != nil {
		// Repository는 성공했으므로 에러 무시 가능
		// 다음 조회 시 캐시에 다시 로드됨
	}

	return nil
}

func (wtc *WriteThroughCache) writeAsync(ctx context.Context, key string, value []byte) error {
	// 캐시에 먼저 저장 (빠른 응답)
	if err := wtc.cache.Set(ctx, key, value, WithTTL(wtc.ttl)); err != nil {
		return err
	}

	// Repository에 비동기 저장
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := wtc.repo.Set(ctx, key, value); err != nil {
			// 실패 시 로깅 또는 재시도 큐에 추가
			// 여기서는 단순히 무시
		}
	}()

	return nil
}

// SetMany는 여러 데이터를 한번에 저장합니다.
func (wtc *WriteThroughCache) SetMany(ctx context.Context, items map[string][]byte) error {
	switch wtc.writeMode {
	case WriteModeSync:
		// Repository에 먼저 저장
		if err := wtc.repo.SetMany(ctx, items); err != nil {
			return err
		}

		// 캐시에 저장
		for key, value := range items {
			wtc.cache.Set(ctx, key, value, WithTTL(wtc.ttl))
		}
		return nil

	case WriteModeAsync:
		// 캐시에 먼저 저장
		for key, value := range items {
			wtc.cache.Set(ctx, key, value, WithTTL(wtc.ttl))
		}

		// Repository에 비동기 저장
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			wtc.repo.SetMany(ctx, items)
		}()
		return nil

	default:
		return nil
	}
}

// Delete는 캐시와 Repository 모두에서 삭제합니다.
func (wtc *WriteThroughCache) Delete(ctx context.Context, key string) error {
	// 동시에 삭제
	var repoErr error
	if wtc.writeMode == WriteModeSync {
		repoErr = wtc.repo.Delete(ctx, key)
	} else {
		go wtc.repo.Delete(context.Background(), key)
	}

	wtc.cache.Delete(ctx, key)
	return repoErr
}

// =============================================================================
// CacheAside: Cache-Aside 패턴 헬퍼
// =============================================================================
// Cache-Aside는 애플리케이션이 캐시와 DB를 직접 관리하는 패턴입니다.
// 이 헬퍼는 공통 로직을 추상화합니다.

// CacheAside는 Cache-Aside 패턴 헬퍼입니다.
type CacheAside[T any] struct {
	cache        *Cache
	ttl          time.Duration
	serializer   func(T) ([]byte, error)
	deserializer func([]byte) (T, error)
}

// NewCacheAside는 새로운 Cache-Aside 헬퍼를 생성합니다.
func NewCacheAside[T any](
	cache *Cache,
	ttl time.Duration,
	serializer func(T) ([]byte, error),
	deserializer func([]byte) (T, error),
) *CacheAside[T] {
	return &CacheAside[T]{
		cache:        cache,
		ttl:          ttl,
		serializer:   serializer,
		deserializer: deserializer,
	}
}

// GetOrLoad는 캐시를 조회하고, 미스 시 loader를 호출합니다.
func (ca *CacheAside[T]) GetOrLoad(ctx context.Context, key string, loader func(context.Context) (T, error)) (T, error) {
	var zero T

	// 캐시 조회
	var data []byte
	layer, _ := ca.cache.Get(ctx, key, &data)
	if layer >= 0 {
		return ca.deserializer(data)
	}

	// 로드
	value, err := loader(ctx)
	if err != nil {
		return zero, err
	}

	// 캐시에 저장
	serialized, err := ca.serializer(value)
	if err != nil {
		return value, nil // 직렬화 실패해도 값은 반환
	}

	ca.cache.Set(ctx, key, serialized, WithTTL(ca.ttl))
	return value, nil
}

// Set은 캐시에 저장합니다.
func (ca *CacheAside[T]) Set(ctx context.Context, key string, value T) error {
	data, err := ca.serializer(value)
	if err != nil {
		return err
	}
	return ca.cache.Set(ctx, key, data, WithTTL(ca.ttl))
}

// Invalidate는 캐시를 무효화합니다.
func (ca *CacheAside[T]) Invalidate(ctx context.Context, key string) error {
	return ca.cache.Delete(ctx, key)
}
