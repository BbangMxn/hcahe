// Package hcache는 지능형 계층적 캐싱 라이브러리입니다.
//
// 특징:
//   - 다중 계층 캐시 (Memory, Redis, PostgreSQL)
//   - 메모리 없이 DB만으로도 동작 가능
//   - 예측 기반 프리페칭
//   - 적응형 TTL
//   - 태그/의존성 기반 스마트 무효화
//   - 다양한 저장 모드 (CacheOnly, WriteThrough, WriteBehind)
//
// 기본 사용법:
//
//	// 가장 간단한 사용 - 메모리 캐시
//	cache := hcache.Quick()
//	cache.Set(ctx, "key", value)
//
//	// Redis만 사용 (메모리 없음)
//	cache, _ := hcache.WithRedis(os.Getenv("REDIS_ADDR"), os.Getenv("REDIS_PASSWORD"), 0)
//
//	// Memory + Redis 2계층
//	cache, _ := hcache.WithMemoryAndRedis(10000, redisAddr, redisPassword, 0)
//
//	// 빌더 패턴으로 세밀한 설정
//	cache, _ := hcache.NewBuilder().
//	    AddMemoryLayer(10000, time.Minute).
//	    AddRedisLayer(redisAddr, redisPassword, 0, 10*time.Minute).
//	    EnablePrediction(3).
//	    Build()
package hcache

import (
	"context"
	"time"

	"github.com/bridgify/hcache/adapters/memory"
	"github.com/bridgify/hcache/adapters/postgres"
	"github.com/bridgify/hcache/adapters/redis"
	"github.com/bridgify/hcache/compression"
	"github.com/bridgify/hcache/core"
	"github.com/bridgify/hcache/serializer"
)

// =============================================================================
// Re-export: 외부에서 사용할 타입들
// =============================================================================

// WriteMode는 캐시 쓰기 모드입니다.
type WriteMode = core.WriteMode

const (
	Hot  = core.Hot
	Warm = core.Warm
	Cold = core.Cold
)

// PersistMode는 영구 저장 모드입니다.
type PersistMode = core.PersistMode

const (
	CacheOnly    = core.CacheOnly
	WriteThrough = core.WriteThrough
	WriteBehind  = core.WriteBehind
	ReadOnly     = core.ReadOnly
)

// Cache는 기본 캐시 인터페이스입니다.
type Cache = core.Cache

// IntelligentCache는 지능형 캐시입니다.
type IntelligentCache = core.IntelligentCache

// Entry는 캐시 엔트리입니다.
type Entry = core.Entry

// =============================================================================
// Quick Start: 빠른 시작
// =============================================================================

// Quick은 기본 설정의 메모리 캐시를 생성합니다.
func Quick() *Cache {
	cache := core.New(
		core.WithDefaultTTL(10*time.Minute),
		core.WithDefaultWriteMode(core.Hot),
	)

	adapter := memory.New(&memory.Config{
		MaxSize:         10000,
		CleanupInterval: time.Minute,
	})

	cache.AddLayer(adapter, core.DefaultLayerOptions("memory", 0))
	return cache
}

// QuickWithSize는 지정된 크기의 메모리 캐시를 생성합니다.
func QuickWithSize(maxSize int) *Cache {
	cache := core.New(
		core.WithDefaultTTL(10*time.Minute),
		core.WithDefaultWriteMode(core.Hot),
	)

	adapter := memory.New(&memory.Config{
		MaxSize:         maxSize,
		CleanupInterval: time.Minute,
	})

	cache.AddLayer(adapter, core.DefaultLayerOptions("memory", 0))
	return cache
}

// =============================================================================
// Single Layer: 단일 계층 캐시
// =============================================================================

// WithMemory는 메모리 전용 캐시를 생성합니다.
func WithMemory(maxSize int, ttl time.Duration) *Cache {
	cache := core.New(
		core.WithDefaultTTL(ttl),
		core.WithDefaultWriteMode(core.Hot),
	)

	adapter := memory.New(&memory.Config{
		MaxSize:         maxSize,
		CleanupInterval: time.Minute,
	})

	cache.AddLayer(adapter, core.DefaultLayerOptions("memory", 0))
	return cache
}

// WithRedis는 Redis 전용 캐시를 생성합니다.
// 메모리 없이 Redis만 사용합니다.
func WithRedis(addr, password string, db int) (*Cache, error) {
	cache := core.New(
		core.WithDefaultTTL(10*time.Minute),
		core.WithDefaultWriteMode(core.Hot),
	)

	adapter := redis.New(&redis.Config{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if err := adapter.Connect(context.Background()); err != nil {
		return nil, err
	}

	cache.AddLayer(adapter, core.DefaultLayerOptions("redis", 0))
	return cache, nil
}

// WithPostgres는 PostgreSQL 전용 캐시를 생성합니다.
// 메모리 없이 PostgreSQL만 사용합니다.
func WithPostgres(connString string) (*Cache, error) {
	cache := core.New(
		core.WithDefaultTTL(1*time.Hour),
		core.WithDefaultWriteMode(core.Hot),
	)

	adapter := postgres.New(&postgres.Config{
		DSN:       connString,
		TableName: "hcache",
	})

	if err := adapter.Connect(context.Background()); err != nil {
		return nil, err
	}

	cache.AddLayer(adapter, core.DefaultLayerOptions("postgres", 0))
	return cache, nil
}

// =============================================================================
// Multi Layer: 다중 계층 캐시
// =============================================================================

// WithMemoryAndRedis는 Memory + Redis 2계층 캐시를 생성합니다.
func WithMemoryAndRedis(memorySize int, redisAddr, redisPassword string, redisDB int) (*Cache, error) {
	cache := core.New(
		core.WithDefaultTTL(10*time.Minute),
		core.WithDefaultWriteMode(core.Hot),
		core.WithAutoTiering(5, 5*time.Minute),
	)

	// L1: Memory
	memAdapter := memory.New(&memory.Config{
		MaxSize:         memorySize,
		CleanupInterval: time.Minute,
	})
	cache.AddLayer(memAdapter, &core.LayerOptions{
		Name:     "L1-Memory",
		Priority: 0,
		TTL:      time.Minute,
	})

	// L2: Redis
	redisAdapter := redis.New(&redis.Config{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	if err := redisAdapter.Connect(context.Background()); err != nil {
		return nil, err
	}
	cache.AddLayer(redisAdapter, &core.LayerOptions{
		Name:     "L2-Redis",
		Priority: 1,
		TTL:      10 * time.Minute,
	})

	return cache, nil
}

// WithMemoryAndPostgres는 Memory + PostgreSQL 2계층 캐시를 생성합니다.
func WithMemoryAndPostgres(memorySize int, pgConnString string) (*Cache, error) {
	cache := core.New(
		core.WithDefaultTTL(1*time.Hour),
		core.WithDefaultWriteMode(core.Hot),
		core.WithAutoTiering(5, 5*time.Minute),
	)

	// L1: Memory
	memAdapter := memory.New(&memory.Config{
		MaxSize:         memorySize,
		CleanupInterval: time.Minute,
	})
	cache.AddLayer(memAdapter, &core.LayerOptions{
		Name:     "L1-Memory",
		Priority: 0,
		TTL:      time.Minute,
	})

	// L2: PostgreSQL
	pgAdapter := postgres.New(&postgres.Config{
		DSN:       pgConnString,
		TableName: "hcache",
	})
	if err := pgAdapter.Connect(context.Background()); err != nil {
		return nil, err
	}
	cache.AddLayer(pgAdapter, &core.LayerOptions{
		Name:     "L2-PostgreSQL",
		Priority: 1,
		TTL:      time.Hour,
	})

	return cache, nil
}

// WithRedisAndPostgres는 Redis + PostgreSQL 2계층 캐시를 생성합니다.
// 메모리 없이 외부 저장소만 사용합니다.
func WithRedisAndPostgres(redisAddr, redisPassword string, redisDB int, pgConnString string) (*Cache, error) {
	cache := core.New(
		core.WithDefaultTTL(1*time.Hour),
		core.WithDefaultWriteMode(core.Hot),
		core.WithAutoTiering(5, 10*time.Minute),
	)

	// L1: Redis
	redisAdapter := redis.New(&redis.Config{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	if err := redisAdapter.Connect(context.Background()); err != nil {
		return nil, err
	}
	cache.AddLayer(redisAdapter, &core.LayerOptions{
		Name:     "L1-Redis",
		Priority: 0,
		TTL:      10 * time.Minute,
	})

	// L2: PostgreSQL
	pgAdapter := postgres.New(&postgres.Config{
		DSN:       pgConnString,
		TableName: "hcache",
	})
	if err := pgAdapter.Connect(context.Background()); err != nil {
		return nil, err
	}
	cache.AddLayer(pgAdapter, &core.LayerOptions{
		Name:     "L2-PostgreSQL",
		Priority: 1,
		TTL:      24 * time.Hour,
	})

	return cache, nil
}

// WithThreeLayers는 Memory + Redis + PostgreSQL 3계층 캐시를 생성합니다.
func WithThreeLayers(memorySize int, redisAddr, redisPassword string, redisDB int, pgConnString string) (*Cache, error) {
	cache := core.New(
		core.WithDefaultTTL(1*time.Hour),
		core.WithDefaultWriteMode(core.Hot),
		core.WithAutoTiering(5, 5*time.Minute),
	)

	// L1: Memory
	memAdapter := memory.New(&memory.Config{
		MaxSize:         memorySize,
		CleanupInterval: time.Minute,
	})
	cache.AddLayer(memAdapter, &core.LayerOptions{
		Name:     "L1-Memory",
		Priority: 0,
		TTL:      30 * time.Second,
	})

	// L2: Redis
	redisAdapter := redis.New(&redis.Config{
		Addr:     redisAddr,
		Password: redisPassword,
		DB:       redisDB,
	})
	if err := redisAdapter.Connect(context.Background()); err != nil {
		return nil, err
	}
	cache.AddLayer(redisAdapter, &core.LayerOptions{
		Name:     "L2-Redis",
		Priority: 1,
		TTL:      10 * time.Minute,
	})

	// L3: PostgreSQL
	pgAdapter := postgres.New(&postgres.Config{
		DSN:       pgConnString,
		TableName: "hcache",
	})
	if err := pgAdapter.Connect(context.Background()); err != nil {
		return nil, err
	}
	cache.AddLayer(pgAdapter, &core.LayerOptions{
		Name:     "L3-PostgreSQL",
		Priority: 2,
		TTL:      24 * time.Hour,
	})

	return cache, nil
}

// =============================================================================
// Intelligent Cache: 지능형 캐시
// =============================================================================

// IntelligentOption은 지능형 캐시 옵션입니다.
type IntelligentOption = core.IntelligentOption

// Intelligent는 기본 메모리 레이어를 가진 지능형 캐시를 생성합니다.
func Intelligent(opts ...IntelligentOption) *IntelligentCache {
	cacheOpts := []core.CacheOption{
		core.WithDefaultTTL(10 * time.Minute),
		core.WithDefaultWriteMode(core.Hot),
	}

	ic := core.NewIntelligentCache(cacheOpts, opts...)

	adapter := memory.New(&memory.Config{
		MaxSize:         10000,
		CleanupInterval: time.Minute,
	})
	ic.AddLayer(adapter, core.DefaultLayerOptions("memory", 0))

	return ic
}

// =============================================================================
// Intelligent Options
// =============================================================================

// WithPrediction은 예측 캐싱을 활성화합니다.
func WithPrediction() IntelligentOption {
	return core.WithPredictiveCaching(3, 5*time.Minute)
}

// WithSmartTTL은 적응형 TTL을 활성화합니다.
func WithSmartTTL(minTTL, maxTTL time.Duration) IntelligentOption {
	return core.WithAdaptiveTTL(minTTL, maxTTL)
}

// WithDependencies는 의존성 추적을 활성화합니다.
func WithDependencies() IntelligentOption {
	return core.WithDependencyTracking(5)
}

// WithTags는 태그 지원을 활성화합니다.
func WithTags() IntelligentOption {
	return core.WithTagSupport(true)
}

// =============================================================================
// Builder Pattern: 세밀한 설정
// =============================================================================

// Builder는 캐시를 구성하는 빌더입니다.
type Builder struct {
	cacheOpts       []core.CacheOption
	intelligentOpts []IntelligentOption
	layers          []layerConfig
	serializer      core.Serializer
	compressor      core.Compressor
}

type layerConfig struct {
	adapter core.Adapter
	options *core.LayerOptions
}

// NewBuilder는 새로운 빌더를 생성합니다.
func NewBuilder() *Builder {
	return &Builder{
		cacheOpts:       make([]core.CacheOption, 0),
		intelligentOpts: make([]IntelligentOption, 0),
		layers:          make([]layerConfig, 0),
	}
}

// WithDefaultTTL은 기본 TTL을 설정합니다.
func (b *Builder) WithDefaultTTL(ttl time.Duration) *Builder {
	b.cacheOpts = append(b.cacheOpts, core.WithDefaultTTL(ttl))
	return b
}

// WithWriteMode는 기본 쓰기 모드를 설정합니다.
func (b *Builder) WithWriteMode(mode WriteMode) *Builder {
	b.cacheOpts = append(b.cacheOpts, core.WithDefaultWriteMode(mode))
	return b
}

// AddMemoryLayer는 메모리 레이어를 추가합니다.
func (b *Builder) AddMemoryLayer(maxSize int, ttl time.Duration) *Builder {
	adapter := memory.New(&memory.Config{
		MaxSize:         maxSize,
		CleanupInterval: time.Minute,
	})

	priority := len(b.layers)
	b.layers = append(b.layers, layerConfig{
		adapter: adapter,
		options: &core.LayerOptions{
			Name:     "Memory",
			Priority: priority,
			TTL:      ttl,
		},
	})
	return b
}

// AddRedisLayer는 Redis 레이어를 추가합니다.
func (b *Builder) AddRedisLayer(addr, password string, db int, ttl time.Duration) *Builder {
	adapter := redis.New(&redis.Config{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	priority := len(b.layers)
	b.layers = append(b.layers, layerConfig{
		adapter: adapter,
		options: &core.LayerOptions{
			Name:     "Redis",
			Priority: priority,
			TTL:      ttl,
		},
	})
	return b
}

// AddPostgresLayer는 PostgreSQL 레이어를 추가합니다.
func (b *Builder) AddPostgresLayer(connString string, ttl time.Duration) *Builder {
	adapter := postgres.New(&postgres.Config{
		DSN:       connString,
		TableName: "hcache",
	})

	priority := len(b.layers)
	b.layers = append(b.layers, layerConfig{
		adapter: adapter,
		options: &core.LayerOptions{
			Name:     "PostgreSQL",
			Priority: priority,
			TTL:      ttl,
		},
	})
	return b
}

// EnablePrediction은 예측 캐싱을 활성화합니다.
func (b *Builder) EnablePrediction(threshold int) *Builder {
	b.intelligentOpts = append(b.intelligentOpts, core.WithPredictiveCaching(threshold, 5*time.Minute))
	return b
}

// EnableAdaptiveTTL은 적응형 TTL을 활성화합니다.
func (b *Builder) EnableAdaptiveTTL(minTTL, maxTTL time.Duration) *Builder {
	b.intelligentOpts = append(b.intelligentOpts, core.WithAdaptiveTTL(minTTL, maxTTL))
	return b
}

// EnableDependencyTracking은 의존성 추적을 활성화합니다.
func (b *Builder) EnableDependencyTracking() *Builder {
	b.intelligentOpts = append(b.intelligentOpts, core.WithDependencyTracking(5))
	return b
}

// EnableTags는 태그 지원을 활성화합니다.
func (b *Builder) EnableTags() *Builder {
	b.intelligentOpts = append(b.intelligentOpts, core.WithTagSupport(true))
	return b
}

// WithJSONSerializer는 JSON 직렬화를 설정합니다.
func (b *Builder) WithJSONSerializer() *Builder {
	b.serializer = serializer.NewJSON()
	return b
}

// WithMsgPackSerializer는 MsgPack 직렬화를 설정합니다.
func (b *Builder) WithMsgPackSerializer() *Builder {
	b.serializer = serializer.NewMsgPack()
	return b
}

// WithGzipCompression은 Gzip 압축을 설정합니다.
func (b *Builder) WithGzipCompression() *Builder {
	b.compressor = compression.NewGzip()
	return b
}

// WithZstdCompression은 Zstd 압축을 설정합니다.
func (b *Builder) WithZstdCompression() *Builder {
	if c, err := compression.NewZstd(); err == nil {
		b.compressor = c
	}
	return b
}

// Build는 지능형 캐시를 생성합니다.
func (b *Builder) Build() (*IntelligentCache, error) {
	ctx := context.Background()

	ic := core.NewIntelligentCache(b.cacheOpts, b.intelligentOpts...)

	if b.serializer != nil {
		ic.SetSerializer(b.serializer)
	}
	if b.compressor != nil {
		ic.SetCompressor(b.compressor)
	}

	for _, layer := range b.layers {
		if err := layer.adapter.Connect(ctx); err != nil {
			return nil, err
		}
		ic.AddLayer(layer.adapter, layer.options)
	}

	// 레이어가 없으면 기본 메모리 레이어
	if len(b.layers) == 0 {
		adapter := memory.New(&memory.Config{
			MaxSize:         10000,
			CleanupInterval: time.Minute,
		})
		ic.AddLayer(adapter, core.DefaultLayerOptions("memory", 0))
	}

	return ic, nil
}

// BuildBasic은 기본 캐시를 생성합니다 (지능형 기능 없음).
func (b *Builder) BuildBasic() (*Cache, error) {
	ctx := context.Background()

	cache := core.New(b.cacheOpts...)

	if b.serializer != nil {
		cache.SetSerializer(b.serializer)
	}
	if b.compressor != nil {
		cache.SetCompressor(b.compressor)
	}

	for _, layer := range b.layers {
		if err := layer.adapter.Connect(ctx); err != nil {
			return nil, err
		}
		cache.AddLayer(layer.adapter, layer.options)
	}

	if len(b.layers) == 0 {
		adapter := memory.New(&memory.Config{
			MaxSize:         10000,
			CleanupInterval: time.Minute,
		})
		cache.AddLayer(adapter, core.DefaultLayerOptions("memory", 0))
	}

	return cache, nil
}
