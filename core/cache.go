// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 메인 캐시 엔진을 구현합니다.
package core

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// =============================================================================
// Cache: 계층적 캐시 메인 엔진
// =============================================================================
// Cache는 여러 Layer를 관리하고, 읽기/쓰기 시 계층 간 데이터 흐름을 조율합니다.
// 기본적으로 Hot 모드로 동작하여 L1에만 동기 저장하고,
// 나머지 계층은 비동기로 전파합니다.
// =============================================================================

// Cache는 계층적 캐시의 메인 엔진입니다.
type Cache struct {
	// layers는 우선순위 순으로 정렬된 캐시 계층입니다.
	// layers[0]이 L1 (가장 빠른 계층)입니다.
	layers []*Layer

	// options는 전역 캐시 옵션입니다.
	options *CacheOptions

	// serializer는 데이터 직렬화/역직렬화를 담당합니다.
	serializer Serializer

	// compressor는 데이터 압축/해제를 담당합니다.
	compressor Compressor

	// writeBuffer는 비동기 쓰기를 위한 버퍼입니다.
	writeBuffer *WriteBuffer

	// plugins는 등록된 플러그인들입니다.
	plugins []Plugin

	// mu는 계층 추가/삭제를 위한 뮤텍스입니다.
	mu sync.RWMutex

	// closed는 캐시가 종료되었는지 여부입니다.
	closed bool
}

// =============================================================================
// Serializer Interface (직렬화)
// =============================================================================

// Serializer는 데이터 직렬화 인터페이스입니다.
type Serializer interface {
	Serialize(v interface{}) ([]byte, error)
	Deserialize(data []byte, v interface{}) error
	Name() string
}

// =============================================================================
// Compressor Interface (압축)
// =============================================================================

// Compressor는 데이터 압축 인터페이스입니다.
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Name() string
}

// =============================================================================
// Plugin Interface (플러그인)
// =============================================================================

// Plugin은 캐시 이벤트를 후킹하는 플러그인 인터페이스입니다.
type Plugin interface {
	Name() string
	OnGet(key string, entry *Entry, layer int, hit bool)
	OnSet(key string, entry *Entry, layers []int)
	OnDelete(key string, layers []int)
	OnEviction(key string, layer int, reason string)
	OnPromotion(key string, from, to int)
	OnDemotion(key string, from, to int)
}

// =============================================================================
// Cache 생성자
// =============================================================================

// New는 새로운 계층적 캐시를 생성합니다.
//
// Example:
//
//	cache := core.New(
//	    core.WithDefaultTTL(10 * time.Minute),
//	    core.WithDefaultWriteMode(core.Hot),
//	)
//	cache.AddLayer(memoryAdapter, core.DefaultLayerOptions("memory", 0))
//	cache.AddLayer(redisAdapter, core.DefaultLayerOptions("redis", 1))
func New(opts ...CacheOption) *Cache {
	options := DefaultCacheOptions()
	for _, opt := range opts {
		opt(options)
	}

	cache := &Cache{
		layers:  make([]*Layer, 0),
		options: options,
		plugins: make([]Plugin, 0),
	}

	// Write Buffer 초기화
	cache.writeBuffer = NewWriteBuffer(
		options.WriteBufferSize,
		options.WriteBufferFlushInterval,
		cache.flushToLayers,
	)

	return cache
}

// =============================================================================
// Cache 계층 관리
// =============================================================================

// AddLayer는 새로운 계층을 추가합니다.
// 계층은 Priority 순으로 자동 정렬됩니다.
func (c *Cache) AddLayer(adapter Adapter, options *LayerOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("cache is closed")
	}

	layer := NewLayer(adapter, options)
	c.layers = append(c.layers, layer)

	// Priority 순으로 정렬
	sort.Slice(c.layers, func(i, j int) bool {
		return c.layers[i].Priority() < c.layers[j].Priority()
	})

	return nil
}

// RemoveLayer는 계층을 제거합니다.
func (c *Cache) RemoveLayer(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, layer := range c.layers {
		if layer.Name() == name {
			// 연결 종료
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			layer.Disconnect(ctx)

			// 슬라이스에서 제거
			c.layers = append(c.layers[:i], c.layers[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("layer not found: %s", name)
}

// Layers는 모든 계층을 반환합니다.
func (c *Cache) Layers() []*Layer {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*Layer, len(c.layers))
	copy(result, c.layers)
	return result
}

// LayerCount는 계층 개수를 반환합니다.
func (c *Cache) LayerCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.layers)
}

// =============================================================================
// Cache 연결 관리
// =============================================================================

// Connect는 모든 계층에 연결합니다.
func (c *Cache) Connect(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, layer := range c.layers {
		if err := layer.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect layer %s: %w", layer.Name(), err)
		}
	}

	return nil
}

// Disconnect는 모든 계층의 연결을 종료합니다.
func (c *Cache) Disconnect(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var lastErr error
	for _, layer := range c.layers {
		if err := layer.Disconnect(ctx); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// =============================================================================
// Cache CRUD 연산
// =============================================================================

// Get은 키에 해당하는 값을 조회합니다.
// L1부터 순차적으로 조회하고, 상위 계층에서 찾으면 하위 계층은 조회하지 않습니다.
// 하위 계층에서 찾은 경우 상위 계층으로 승격(promotion)합니다.
//
// Returns:
//   - value: 조회된 값 (역직렬화된 상태)
//   - layer: 값을 찾은 계층 인덱스 (-1이면 못 찾음)
//   - error: 에러
func (c *Cache) Get(ctx context.Context, key string, dest interface{}) (int, error) {
	// 핫 패스: 락 없이 직접 접근 (layers는 append-only)
	layers := c.layers
	if len(layers) == 0 {
		return -1, fmt.Errorf("no layers configured")
	}

	// 플러그인 여부 미리 확인
	hasPlugins := len(c.plugins) > 0

	// 각 계층을 순차적으로 조회
	for i, layer := range layers {
		// SkipOnMiss 옵션 확인
		if layer.Options().SkipOnMiss {
			continue
		}

		entry, hit, err := layer.Get(ctx, key)
		if err != nil {
			// 에러 발생해도 다음 계층 시도
			continue
		}

		if hit {
			// 역직렬화
			data := entry.Value

			// 압축 해제
			if entry.Compressed && c.compressor != nil {
				decompressed, err := c.compressor.Decompress(data)
				if err != nil {
					return -1, fmt.Errorf("decompress error: %w", err)
				}
				data = decompressed
			}

			// 역직렬화
			if c.serializer != nil {
				if err := c.serializer.Deserialize(data, dest); err != nil {
					return -1, fmt.Errorf("deserialize error: %w", err)
				}
			}

			// 플러그인 호출 (있는 경우만)
			if hasPlugins {
				for _, plugin := range c.plugins {
					plugin.OnGet(key, entry, i, true)
				}
			}

			// 상위 계층으로 승격 (i > 0이면 하위 계층에서 찾은 것)
			if i > 0 && c.options.EnableAutoTiering {
				go c.promoteToUpper(ctx, entry, i)
			}

			return i, nil
		}

		// 플러그인 호출 (미스) - 있는 경우만
		if hasPlugins {
			for _, plugin := range c.plugins {
				plugin.OnGet(key, nil, i, false)
			}
		}
	}

	return -1, nil // 못 찾음 (에러 아님)
}

// GetRaw는 직렬화된 상태 그대로 조회합니다.
func (c *Cache) GetRaw(ctx context.Context, key string) (*Entry, int, error) {
	// 핫 패스: 락 없이 직접 접근
	layers := c.layers

	for i, layer := range layers {
		entry, hit, err := layer.Get(ctx, key)
		if err != nil {
			continue
		}
		if hit {
			return entry, i, nil
		}
	}

	return nil, -1, nil
}

// Set은 값을 저장합니다.
// WriteMode에 따라 동기/비동기 저장이 결정됩니다.
//
// Parameters:
//   - ctx: 컨텍스트
//   - key: 캐시 키
//   - value: 저장할 값 (직렬화됨)
//   - opts: 저장 옵션
func (c *Cache) Set(ctx context.Context, key string, value interface{}, opts ...SetOption) error {
	c.mu.RLock()
	layers := c.layers
	options := c.options
	c.mu.RUnlock()

	if len(layers) == 0 {
		return fmt.Errorf("no layers configured")
	}

	// 옵션 처리
	setOpts := DefaultSetOptions()
	for _, opt := range opts {
		opt(setOpts)
	}

	// WriteMode 결정
	mode := options.DefaultWriteMode
	if setOpts.Mode != nil {
		mode = *setOpts.Mode
	}

	// TTL 결정
	ttl := options.DefaultTTL
	if setOpts.TTL != 0 {
		ttl = setOpts.TTL
	}
	if setOpts.TTL == -1 {
		ttl = 0 // 만료 없음
	}

	// 직렬화
	var data []byte
	var err error
	if c.serializer != nil {
		data, err = c.serializer.Serialize(value)
		if err != nil {
			return fmt.Errorf("serialize error: %w", err)
		}
	} else {
		// 직렬화기 없으면 바이트 슬라이스로 캐스팅 시도
		var ok bool
		data, ok = value.([]byte)
		if !ok {
			return fmt.Errorf("no serializer configured and value is not []byte")
		}
	}

	// 압축
	compressed := false
	compressionType := ""
	if c.compressor != nil && options.CompressionEnabled && !setOpts.SkipCompression {
		if len(data) >= options.CompressionThreshold {
			compressedData, err := c.compressor.Compress(data)
			if err == nil && len(compressedData) < len(data) {
				data = compressedData
				compressed = true
				compressionType = c.compressor.Name()
			}
		}
	}

	// Entry 생성
	entry := NewEntry(key, data, ttl)
	entry.Compressed = compressed
	entry.CompressionType = compressionType

	// 대상 계층 결정
	targetLayers := setOpts.TargetLayers
	if targetLayers == nil {
		targetLayers = make([]int, len(layers))
		for i := range layers {
			targetLayers[i] = i
		}
	}

	// WriteMode에 따른 저장
	switch mode {
	case Hot:
		// L1에만 동기 저장, 나머지는 비동기
		return c.writeHot(ctx, entry, targetLayers)

	case Warm:
		// L1, L2까지 동기 저장, 나머지는 비동기
		return c.writeWarm(ctx, entry, targetLayers)

	case Cold:
		// 모든 계층에 동기 저장
		return c.writeCold(ctx, entry, targetLayers)

	default:
		return c.writeHot(ctx, entry, targetLayers)
	}
}

// SetRaw는 Entry를 직접 저장합니다.
func (c *Cache) SetRaw(ctx context.Context, entry *Entry, opts ...SetOption) error {
	setOpts := DefaultSetOptions()
	for _, opt := range opts {
		opt(setOpts)
	}

	mode := c.options.DefaultWriteMode
	if setOpts.Mode != nil {
		mode = *setOpts.Mode
	}

	targetLayers := setOpts.TargetLayers
	if targetLayers == nil {
		targetLayers = make([]int, len(c.layers))
		for i := range c.layers {
			targetLayers[i] = i
		}
	}

	switch mode {
	case Hot:
		return c.writeHot(ctx, entry, targetLayers)
	case Warm:
		return c.writeWarm(ctx, entry, targetLayers)
	case Cold:
		return c.writeCold(ctx, entry, targetLayers)
	default:
		return c.writeHot(ctx, entry, targetLayers)
	}
}

// Delete는 모든 계층에서 키를 삭제합니다.
func (c *Cache) Delete(ctx context.Context, key string) error {
	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	deletedLayers := make([]int, 0)

	for i, layer := range layers {
		if err := layer.Delete(ctx, key); err != nil {
			// 에러가 발생해도 계속 진행
			continue
		}
		deletedLayers = append(deletedLayers, i)
	}

	// 플러그인 호출
	for _, plugin := range c.plugins {
		plugin.OnDelete(key, deletedLayers)
	}

	return nil
}

// Has는 키가 존재하는지 확인합니다 (아무 계층이나).
func (c *Cache) Has(ctx context.Context, key string) (bool, error) {
	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	for _, layer := range layers {
		exists, err := layer.Has(ctx, key)
		if err != nil {
			continue
		}
		if exists {
			return true, nil
		}
	}

	return false, nil
}

// =============================================================================
// Cache 내부 쓰기 메서드
// =============================================================================

// writeHot은 Hot 모드로 저장합니다.
// L1에만 동기 저장하고, 나머지는 비동기로 전파합니다.
func (c *Cache) writeHot(ctx context.Context, entry *Entry, targetLayers []int) error {
	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	if len(targetLayers) == 0 {
		return nil
	}

	// L1 동기 저장
	firstLayer := targetLayers[0]
	if firstLayer < len(layers) {
		if err := layers[firstLayer].Set(ctx, entry); err != nil {
			return fmt.Errorf("L1 write failed: %w", err)
		}
	}

	// 나머지 계층 비동기 저장 (Write Buffer 사용)
	if len(targetLayers) > 1 {
		asyncLayers := targetLayers[1:]
		c.writeBuffer.Add(entry, asyncLayers)
	}

	// 플러그인 호출
	for _, plugin := range c.plugins {
		plugin.OnSet(entry.Key, entry, targetLayers)
	}

	return nil
}

// writeWarm은 Warm 모드로 저장합니다.
// L1, L2까지 동기 저장하고, L3 이하는 비동기로 전파합니다.
func (c *Cache) writeWarm(ctx context.Context, entry *Entry, targetLayers []int) error {
	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	syncCount := 2 // L1, L2까지 동기
	if len(targetLayers) < syncCount {
		syncCount = len(targetLayers)
	}

	// 동기 저장
	for i := 0; i < syncCount; i++ {
		layerIdx := targetLayers[i]
		if layerIdx < len(layers) {
			if err := layers[layerIdx].Set(ctx, entry); err != nil {
				return fmt.Errorf("layer %d write failed: %w", layerIdx, err)
			}
		}
	}

	// 나머지 비동기 저장
	if len(targetLayers) > syncCount {
		asyncLayers := targetLayers[syncCount:]
		c.writeBuffer.Add(entry, asyncLayers)
	}

	// 플러그인 호출
	for _, plugin := range c.plugins {
		plugin.OnSet(entry.Key, entry, targetLayers)
	}

	return nil
}

// writeCold는 Cold 모드로 저장합니다.
// 모든 계층에 동기 저장합니다.
func (c *Cache) writeCold(ctx context.Context, entry *Entry, targetLayers []int) error {
	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	for _, layerIdx := range targetLayers {
		if layerIdx < len(layers) {
			if err := layers[layerIdx].Set(ctx, entry); err != nil {
				return fmt.Errorf("layer %d write failed: %w", layerIdx, err)
			}
		}
	}

	// 플러그인 호출
	for _, plugin := range c.plugins {
		plugin.OnSet(entry.Key, entry, targetLayers)
	}

	return nil
}

// flushToLayers는 Write Buffer에서 호출되어 실제 저장을 수행합니다.
func (c *Cache) flushToLayers(entries []*BufferEntry) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	// 계층별로 그룹화
	layerEntries := make(map[int][]*Entry)
	for _, be := range entries {
		for _, layerIdx := range be.TargetLayers {
			if layerEntries[layerIdx] == nil {
				layerEntries[layerIdx] = make([]*Entry, 0)
			}
			layerEntries[layerIdx] = append(layerEntries[layerIdx], be.Entry)
		}
	}

	// 각 계층에 배치 저장
	for layerIdx, ents := range layerEntries {
		if layerIdx < len(layers) {
			layers[layerIdx].SetMany(ctx, ents)
		}
	}
}

// promoteToUpper는 엔트리를 상위 계층으로 승격합니다.
func (c *Cache) promoteToUpper(ctx context.Context, entry *Entry, fromLayer int) {
	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	// 모든 상위 계층에 저장
	for i := 0; i < fromLayer; i++ {
		layers[i].Set(ctx, entry.Clone())
	}

	// 플러그인 호출
	for _, plugin := range c.plugins {
		plugin.OnPromotion(entry.Key, fromLayer, 0)
	}
}

// =============================================================================
// Cache 플러그인 관리
// =============================================================================

// AddPlugin은 플러그인을 추가합니다.
func (c *Cache) AddPlugin(plugin Plugin) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.plugins = append(c.plugins, plugin)
}

// =============================================================================
// Cache Serializer/Compressor 설정
// =============================================================================

// SetSerializer는 직렬화기를 설정합니다.
func (c *Cache) SetSerializer(s Serializer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serializer = s
}

// SetCompressor는 압축기를 설정합니다.
func (c *Cache) SetCompressor(comp Compressor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compressor = comp
}

// =============================================================================
// Cache 관리
// =============================================================================

// Clear는 모든 계층의 모든 엔트리를 삭제합니다.
func (c *Cache) Clear(ctx context.Context) error {
	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	for _, layer := range layers {
		if err := layer.Adapter().Clear(ctx); err != nil {
			return fmt.Errorf("clear layer %s failed: %w", layer.Name(), err)
		}
	}

	return nil
}

// Stats는 모든 계층의 통계를 반환합니다.
func (c *Cache) Stats(ctx context.Context) ([]*LayerStats, error) {
	c.mu.RLock()
	layers := c.layers
	c.mu.RUnlock()

	stats := make([]*LayerStats, len(layers))
	for i, layer := range layers {
		s, err := layer.Stats(ctx)
		if err != nil {
			return nil, fmt.Errorf("stats layer %s failed: %w", layer.Name(), err)
		}
		stats[i] = s
	}

	return stats, nil
}

// Close는 캐시를 종료합니다.
// Write Buffer를 플러시하고 모든 연결을 종료합니다.
func (c *Cache) Close(ctx context.Context) error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	// Write Buffer 플러시 및 종료
	if c.writeBuffer != nil {
		c.writeBuffer.Close()
	}

	// 모든 계층 연결 종료
	return c.Disconnect(ctx)
}
