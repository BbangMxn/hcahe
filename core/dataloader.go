// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 DataLoader 패턴을 구현합니다.
package core

import (
	"context"
	"sync"
	"time"
)

// =============================================================================
// DataLoader: N+1 문제 해결
// =============================================================================
// 짧은 시간 내에 발생하는 여러 요청을 배치로 묶어
// 단일 쿼리로 처리합니다. GraphQL의 DataLoader 패턴을 구현합니다.
//
// 동작:
// 1. 요청 수집 (wait 기간 동안)
// 2. 배치 로더 호출 (단일 쿼리)
// 3. 결과 분배
// =============================================================================

// BatchLoadFunc는 배치 로드 함수입니다.
// 여러 키를 받아 map[key]value를 반환합니다.
type BatchLoadFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// DataLoader는 요청을 배치로 묶어 처리합니다.
type DataLoader[K comparable, V any] struct {
	batchFn BatchLoadFunc[K, V]
	cache   *Cache
	config  *DataLoaderConfig

	// 현재 배치
	batch     *loaderBatch[K, V]
	batchLock sync.Mutex
}

type loaderBatch[K comparable, V any] struct {
	keys    []K
	results map[K]V
	errors  map[K]error
	done    chan struct{}
	once    sync.Once
}

// DataLoaderConfig는 DataLoader 설정입니다.
type DataLoaderConfig struct {
	// Wait는 요청을 모으는 시간입니다.
	Wait time.Duration

	// MaxBatch는 최대 배치 크기입니다.
	MaxBatch int

	// CacheTTL은 캐시 TTL입니다. (0이면 캐시 안함)
	CacheTTL time.Duration
}

// DefaultDataLoaderConfig는 기본 설정을 반환합니다.
func DefaultDataLoaderConfig() *DataLoaderConfig {
	return &DataLoaderConfig{
		Wait:     2 * time.Millisecond, // 2ms 대기
		MaxBatch: 100,
		CacheTTL: time.Minute,
	}
}

// NewDataLoader는 새로운 DataLoader를 생성합니다.
func NewDataLoader[K comparable, V any](
	batchFn BatchLoadFunc[K, V],
	cache *Cache,
	config *DataLoaderConfig,
) *DataLoader[K, V] {
	if config == nil {
		config = DefaultDataLoaderConfig()
	}

	return &DataLoader[K, V]{
		batchFn: batchFn,
		cache:   cache,
		config:  config,
	}
}

// Load는 단일 키를 로드합니다.
func (dl *DataLoader[K, V]) Load(ctx context.Context, key K) (V, error) {
	results, err := dl.LoadMany(ctx, []K{key})
	if err != nil {
		var zero V
		return zero, err
	}

	if val, ok := results[key]; ok {
		return val, nil
	}

	var zero V
	return zero, nil
}

// LoadMany는 여러 키를 로드합니다.
func (dl *DataLoader[K, V]) LoadMany(ctx context.Context, keys []K) (map[K]V, error) {
	if len(keys) == 0 {
		return make(map[K]V), nil
	}

	// 현재 배치 가져오기 또는 새로 생성
	batch := dl.getCurrentBatch()

	// 키 추가
	dl.batchLock.Lock()
	for _, key := range keys {
		batch.keys = append(batch.keys, key)
	}

	// 배치 크기 초과시 즉시 실행
	if len(batch.keys) >= dl.config.MaxBatch {
		dl.dispatchBatch(batch)
		dl.batch = nil
	}
	dl.batchLock.Unlock()

	// 결과 대기
	select {
	case <-batch.done:
		// 결과 수집
		results := make(map[K]V, len(keys))
		for _, key := range keys {
			if val, ok := batch.results[key]; ok {
				results[key] = val
			}
		}

		// 에러 확인
		for _, key := range keys {
			if err, ok := batch.errors[key]; ok {
				return results, err
			}
		}

		return results, nil

	case <-ctx.Done():
		var zero map[K]V
		return zero, ctx.Err()
	}
}

func (dl *DataLoader[K, V]) getCurrentBatch() *loaderBatch[K, V] {
	dl.batchLock.Lock()
	defer dl.batchLock.Unlock()

	if dl.batch == nil {
		dl.batch = &loaderBatch[K, V]{
			keys:    make([]K, 0, dl.config.MaxBatch),
			results: make(map[K]V),
			errors:  make(map[K]error),
			done:    make(chan struct{}),
		}

		// 타이머 시작
		go func(b *loaderBatch[K, V]) {
			time.Sleep(dl.config.Wait)
			dl.batchLock.Lock()
			if dl.batch == b {
				dl.dispatchBatch(b)
				dl.batch = nil
			}
			dl.batchLock.Unlock()
		}(dl.batch)
	}

	return dl.batch
}

func (dl *DataLoader[K, V]) dispatchBatch(batch *loaderBatch[K, V]) {
	batch.once.Do(func() {
		if len(batch.keys) == 0 {
			close(batch.done)
			return
		}

		// 중복 제거
		uniqueKeys := make([]K, 0, len(batch.keys))
		seen := make(map[K]bool)
		for _, key := range batch.keys {
			if !seen[key] {
				seen[key] = true
				uniqueKeys = append(uniqueKeys, key)
			}
		}

		// 배치 로드 실행
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		results, err := dl.batchFn(ctx, uniqueKeys)
		if err != nil {
			for _, key := range uniqueKeys {
				batch.errors[key] = err
			}
		} else {
			batch.results = results
		}

		close(batch.done)
	})
}

// Prime은 캐시에 값을 미리 저장합니다.
func (dl *DataLoader[K, V]) Prime(ctx context.Context, key K, value V) {
	if dl.cache != nil && dl.config.CacheTTL > 0 {
		// 타입에 맞게 직렬화 필요 - 여기서는 단순화
	}
}

// Clear는 캐시를 비웁니다.
func (dl *DataLoader[K, V]) Clear(ctx context.Context, key K) {
	if dl.cache != nil {
		// 캐시 무효화
	}
}

// =============================================================================
// CachedDataLoader: 캐시가 통합된 DataLoader
// =============================================================================

// CachedDataLoader는 캐시가 통합된 DataLoader입니다.
type CachedDataLoader[V any] struct {
	loader *DataLoader[string, V]
	cache  *Cache
	ttl    time.Duration

	// 직렬화
	serialize   func(V) ([]byte, error)
	deserialize func([]byte) (V, error)
}

// CachedDataLoaderConfig는 설정입니다.
type CachedDataLoaderConfig[V any] struct {
	DataLoaderConfig
	Serialize   func(V) ([]byte, error)
	Deserialize func([]byte) (V, error)
}

// NewCachedDataLoader는 새로운 CachedDataLoader를 생성합니다.
func NewCachedDataLoader[V any](
	batchFn BatchLoadFunc[string, V],
	cache *Cache,
	config *CachedDataLoaderConfig[V],
) *CachedDataLoader[V] {
	if config == nil {
		config = &CachedDataLoaderConfig[V]{
			DataLoaderConfig: *DefaultDataLoaderConfig(),
		}
	}

	return &CachedDataLoader[V]{
		loader:      NewDataLoader(batchFn, cache, &config.DataLoaderConfig),
		cache:       cache,
		ttl:         config.CacheTTL,
		serialize:   config.Serialize,
		deserialize: config.Deserialize,
	}
}

// Load는 캐시를 확인한 후 로드합니다.
func (cdl *CachedDataLoader[V]) Load(ctx context.Context, key string) (V, error) {
	var zero V

	// 1. 캐시 확인
	if cdl.cache != nil && cdl.deserialize != nil {
		var data []byte
		layer, _ := cdl.cache.Get(ctx, key, &data)
		if layer >= 0 {
			return cdl.deserialize(data)
		}
	}

	// 2. DataLoader로 로드
	value, err := cdl.loader.Load(ctx, key)
	if err != nil {
		return zero, err
	}

	// 3. 캐시에 저장
	if cdl.cache != nil && cdl.serialize != nil {
		if data, err := cdl.serialize(value); err == nil {
			cdl.cache.Set(ctx, key, data, WithTTL(cdl.ttl))
		}
	}

	return value, nil
}

// LoadMany는 여러 키를 로드합니다.
func (cdl *CachedDataLoader[V]) LoadMany(ctx context.Context, keys []string) (map[string]V, error) {
	results := make(map[string]V, len(keys))
	var missingKeys []string

	// 1. 캐시에서 먼저 조회
	if cdl.cache != nil && cdl.deserialize != nil {
		for _, key := range keys {
			var data []byte
			layer, _ := cdl.cache.Get(ctx, key, &data)
			if layer >= 0 {
				if val, err := cdl.deserialize(data); err == nil {
					results[key] = val
					continue
				}
			}
			missingKeys = append(missingKeys, key)
		}
	} else {
		missingKeys = keys
	}

	// 2. 미스된 키들은 DataLoader로 로드
	if len(missingKeys) > 0 {
		loaded, err := cdl.loader.LoadMany(ctx, missingKeys)
		if err != nil {
			return results, err
		}

		for key, value := range loaded {
			results[key] = value

			// 캐시에 저장
			if cdl.cache != nil && cdl.serialize != nil {
				if data, err := cdl.serialize(value); err == nil {
					cdl.cache.Set(ctx, key, data, WithTTL(cdl.ttl))
				}
			}
		}
	}

	return results, nil
}

// =============================================================================
// RequestScopedLoader: 요청 범위 DataLoader
// =============================================================================
// 각 HTTP 요청마다 새로운 DataLoader 인스턴스를 생성하여
// 요청 내에서만 배치를 공유합니다.

// LoaderFactory는 DataLoader 팩토리입니다.
type LoaderFactory[K comparable, V any] struct {
	batchFn BatchLoadFunc[K, V]
	config  *DataLoaderConfig
}

// NewLoaderFactory는 새로운 LoaderFactory를 생성합니다.
func NewLoaderFactory[K comparable, V any](
	batchFn BatchLoadFunc[K, V],
	config *DataLoaderConfig,
) *LoaderFactory[K, V] {
	return &LoaderFactory[K, V]{
		batchFn: batchFn,
		config:  config,
	}
}

// NewLoader는 새로운 DataLoader 인스턴스를 생성합니다.
func (f *LoaderFactory[K, V]) NewLoader() *DataLoader[K, V] {
	return NewDataLoader(f.batchFn, nil, f.config)
}

// =============================================================================
// Context Key for Request-Scoped Loaders
// =============================================================================

type loaderContextKey struct{}

// LoadersContext는 요청 범위 로더들을 저장합니다.
type LoadersContext struct {
	loaders sync.Map
}

// NewLoadersContext는 새로운 LoadersContext를 생성합니다.
func NewLoadersContext() *LoadersContext {
	return &LoadersContext{}
}

// WithLoaders는 컨텍스트에 LoadersContext를 추가합니다.
func WithLoaders(ctx context.Context, loaders *LoadersContext) context.Context {
	return context.WithValue(ctx, loaderContextKey{}, loaders)
}

// GetLoaders는 컨텍스트에서 LoadersContext를 가져옵니다.
func GetLoaders(ctx context.Context) *LoadersContext {
	if loaders, ok := ctx.Value(loaderContextKey{}).(*LoadersContext); ok {
		return loaders
	}
	return nil
}

// Get은 이름으로 로더를 가져옵니다.
func (lc *LoadersContext) Get(name string) interface{} {
	if loader, ok := lc.loaders.Load(name); ok {
		return loader
	}
	return nil
}

// Set은 로더를 저장합니다.
func (lc *LoadersContext) Set(name string, loader interface{}) {
	lc.loaders.Store(name, loader)
}
