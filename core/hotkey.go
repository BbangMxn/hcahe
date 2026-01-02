// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Hot Key 감지 및 자동 복제를 구현합니다.
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Hot Key Detection: 핫 키 감지
// =============================================================================
// 특정 키에 요청이 집중되면 자동으로 감지하고
// 로컬 복제본을 생성하여 부하를 분산합니다.
//
// 알고리즘:
// - Sliding Window Counter로 요청 빈도 추적
// - 임계값 초과 시 Hot Key로 마킹
// - Hot Key는 로컬 캐시에 복제
// =============================================================================

// HotKeyDetector는 핫 키를 감지합니다.
type HotKeyDetector struct {
	counters sync.Map // key -> *hotKeyCounter
	hotKeys  sync.Map // key -> *HotKeyInfo

	config *HotKeyConfig

	// 통계
	detected   uint64
	replicated uint64
}

type hotKeyCounter struct {
	counts     [60]uint32 // 초당 카운터 (60초 슬라이딩 윈도우)
	currentIdx int32
	lastUpdate int64
	mu         sync.Mutex
}

// HotKeyInfo는 핫 키 정보입니다.
type HotKeyInfo struct {
	Key         string
	DetectedAt  time.Time
	RequestRate float64 // 초당 요청 수
	TotalHits   uint64
	Replicated  bool
}

// HotKeyConfig는 핫 키 감지 설정입니다.
type HotKeyConfig struct {
	// Threshold는 핫 키로 판단하는 초당 요청 수입니다.
	Threshold float64

	// WindowSize는 측정 윈도우 크기입니다.
	WindowSize time.Duration

	// CooldownPeriod는 핫 키 해제까지의 대기 시간입니다.
	CooldownPeriod time.Duration

	// OnHotKey는 핫 키 감지 시 콜백입니다.
	OnHotKey func(info *HotKeyInfo)

	// MaxHotKeys는 최대 핫 키 개수입니다.
	MaxHotKeys int
}

// DefaultHotKeyConfig는 기본 설정을 반환합니다.
func DefaultHotKeyConfig() *HotKeyConfig {
	return &HotKeyConfig{
		Threshold:      100, // 초당 100회 이상
		WindowSize:     time.Minute,
		CooldownPeriod: 5 * time.Minute,
		MaxHotKeys:     100,
	}
}

// NewHotKeyDetector는 새로운 핫 키 감지기를 생성합니다.
func NewHotKeyDetector(config *HotKeyConfig) *HotKeyDetector {
	if config == nil {
		config = DefaultHotKeyConfig()
	}

	detector := &HotKeyDetector{
		config: config,
	}

	// 주기적 정리
	go detector.cleanupLoop()

	return detector
}

// Record는 키 접근을 기록합니다.
func (d *HotKeyDetector) Record(key string) {
	counter := d.getOrCreateCounter(key)
	rate := counter.increment()

	// 임계값 초과 확인
	if rate >= d.config.Threshold {
		d.markAsHot(key, rate)
	}
}

func (d *HotKeyDetector) getOrCreateCounter(key string) *hotKeyCounter {
	if val, ok := d.counters.Load(key); ok {
		return val.(*hotKeyCounter)
	}

	counter := &hotKeyCounter{
		lastUpdate: time.Now().Unix(),
	}

	actual, _ := d.counters.LoadOrStore(key, counter)
	return actual.(*hotKeyCounter)
}

func (c *hotKeyCounter) increment() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	currentIdx := int32(now % 60)

	// 인덱스가 바뀌었으면 해당 슬롯 초기화
	if currentIdx != c.currentIdx {
		// 지나간 슬롯들 초기화
		elapsed := now - c.lastUpdate
		if elapsed > 60 {
			elapsed = 60
		}
		for i := int64(1); i <= elapsed; i++ {
			idx := (c.currentIdx + int32(i)) % 60
			c.counts[idx] = 0
		}
		c.currentIdx = currentIdx
		c.lastUpdate = now
	}

	c.counts[currentIdx]++

	// 최근 10초 평균 계산
	var sum uint32
	for i := 0; i < 10; i++ {
		idx := (currentIdx - int32(i) + 60) % 60
		sum += c.counts[idx]
	}

	return float64(sum) / 10.0
}

func (d *HotKeyDetector) markAsHot(key string, rate float64) {
	if _, exists := d.hotKeys.Load(key); exists {
		return // 이미 핫 키
	}

	// 최대 개수 확인
	hotCount := 0
	d.hotKeys.Range(func(_, _ interface{}) bool {
		hotCount++
		return true
	})
	if hotCount >= d.config.MaxHotKeys {
		return
	}

	info := &HotKeyInfo{
		Key:         key,
		DetectedAt:  time.Now(),
		RequestRate: rate,
	}

	d.hotKeys.Store(key, info)
	atomic.AddUint64(&d.detected, 1)

	if d.config.OnHotKey != nil {
		go d.config.OnHotKey(info)
	}
}

// IsHot은 키가 핫 키인지 확인합니다.
func (d *HotKeyDetector) IsHot(key string) bool {
	_, ok := d.hotKeys.Load(key)
	return ok
}

// GetHotKeys는 모든 핫 키를 반환합니다.
func (d *HotKeyDetector) GetHotKeys() []*HotKeyInfo {
	var keys []*HotKeyInfo
	d.hotKeys.Range(func(_, value interface{}) bool {
		keys = append(keys, value.(*HotKeyInfo))
		return true
	})
	return keys
}

// Stats는 통계를 반환합니다.
func (d *HotKeyDetector) Stats() (detected, replicated uint64) {
	return atomic.LoadUint64(&d.detected), atomic.LoadUint64(&d.replicated)
}

func (d *HotKeyDetector) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()

		// 쿨다운 지난 핫 키 제거
		d.hotKeys.Range(func(key, value interface{}) bool {
			info := value.(*HotKeyInfo)
			if now.Sub(info.DetectedAt) > d.config.CooldownPeriod {
				d.hotKeys.Delete(key)
			}
			return true
		})

		// 오래된 카운터 정리
		d.counters.Range(func(key, value interface{}) bool {
			counter := value.(*hotKeyCounter)
			if now.Unix()-counter.lastUpdate > 300 { // 5분 이상 접근 없음
				d.counters.Delete(key)
			}
			return true
		})
	}
}

// =============================================================================
// HotKeyCache: 핫 키 자동 복제 캐시
// =============================================================================

// HotKeyCache는 핫 키를 자동으로 로컬에 복제하는 캐시입니다.
type HotKeyCache struct {
	cache      *Cache
	localCache *Cache // 로컬 복제용
	detector   *HotKeyDetector

	replicating sync.Map // key -> bool
}

// HotKeyCacheConfig는 설정입니다.
type HotKeyCacheConfig struct {
	HotKeyConfig   *HotKeyConfig
	LocalCacheSize int
	LocalTTL       time.Duration
}

// DefaultHotKeyCacheConfig는 기본 설정을 반환합니다.
func DefaultHotKeyCacheConfig() *HotKeyCacheConfig {
	return &HotKeyCacheConfig{
		HotKeyConfig:   DefaultHotKeyConfig(),
		LocalCacheSize: 1000,
		LocalTTL:       30 * time.Second,
	}
}

// NewHotKeyCache는 새로운 HotKeyCache를 생성합니다.
func NewHotKeyCache(cache *Cache, localAdapter Adapter, config *HotKeyCacheConfig) *HotKeyCache {
	if config == nil {
		config = DefaultHotKeyCacheConfig()
	}

	// 로컬 캐시 설정
	localCache := New(WithDefaultTTL(config.LocalTTL))
	localCache.AddLayer(localAdapter, DefaultLayerOptions("local-replica", 0))

	hkc := &HotKeyCache{
		cache:      cache,
		localCache: localCache,
		detector:   NewHotKeyDetector(config.HotKeyConfig),
	}

	// 핫 키 감지 콜백 설정
	config.HotKeyConfig.OnHotKey = func(info *HotKeyInfo) {
		go hkc.replicateKey(info.Key)
	}

	return hkc
}

// Get은 캐시를 조회합니다. 핫 키는 로컬에서 먼저 조회합니다.
func (hkc *HotKeyCache) Get(ctx context.Context, key string, dest interface{}) (int, error) {
	// 접근 기록
	hkc.detector.Record(key)

	// 핫 키면 로컬 먼저 조회
	if hkc.detector.IsHot(key) {
		layer, err := hkc.localCache.Get(ctx, key, dest)
		if layer >= 0 {
			return layer, err
		}
		// 로컬에 없으면 원본에서 조회 후 복제
		go hkc.replicateKey(key)
	}

	return hkc.cache.Get(ctx, key, dest)
}

// Set은 캐시에 저장합니다.
func (hkc *HotKeyCache) Set(ctx context.Context, key string, value interface{}, opts ...SetOption) error {
	err := hkc.cache.Set(ctx, key, value, opts...)

	// 핫 키면 로컬에도 저장
	if hkc.detector.IsHot(key) {
		hkc.localCache.Set(ctx, key, value, opts...)
	}

	return err
}

func (hkc *HotKeyCache) replicateKey(key string) {
	// 중복 복제 방지
	if _, replicating := hkc.replicating.LoadOrStore(key, true); replicating {
		return
	}
	defer hkc.replicating.Delete(key)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 원본에서 데이터 가져오기
	entry, layer, err := hkc.cache.GetRaw(ctx, key)
	if err != nil || layer < 0 {
		return
	}

	// 로컬에 복제
	hkc.localCache.SetRaw(ctx, entry.Clone())
	atomic.AddUint64(&hkc.detector.replicated, 1)
}

// Detector는 핫 키 감지기를 반환합니다.
func (hkc *HotKeyCache) Detector() *HotKeyDetector {
	return hkc.detector
}

// Cache는 내부 캐시를 반환합니다.
func (hkc *HotKeyCache) Cache() *Cache {
	return hkc.cache
}

// =============================================================================
// Top-K Hot Keys: 상위 K개 핫 키 추적
// =============================================================================

// TopKHotKeys는 상위 K개 핫 키를 추적합니다.
type TopKHotKeys struct {
	k      int
	heap   []*keyCount
	keyMap sync.Map // key -> index in heap
	mu     sync.RWMutex
}

type keyCount struct {
	key   string
	count uint64
}

// NewTopKHotKeys는 새로운 TopK 추적기를 생성합니다.
func NewTopKHotKeys(k int) *TopKHotKeys {
	return &TopKHotKeys{
		k:    k,
		heap: make([]*keyCount, 0, k),
	}
}

// Record는 키 접근을 기록합니다.
func (tk *TopKHotKeys) Record(key string) {
	tk.mu.Lock()
	defer tk.mu.Unlock()

	// 기존 키인지 확인
	if idx, ok := tk.keyMap.Load(key); ok {
		i := idx.(int)
		if i < len(tk.heap) {
			tk.heap[i].count++
			tk.bubbleDown(i)
		}
		return
	}

	// 새 키
	newKC := &keyCount{key: key, count: 1}

	if len(tk.heap) < tk.k {
		// 아직 공간 있음
		tk.heap = append(tk.heap, newKC)
		tk.keyMap.Store(key, len(tk.heap)-1)
		tk.bubbleUp(len(tk.heap) - 1)
	} else if tk.heap[0].count < newKC.count {
		// 최소값보다 큼 - 교체
		oldKey := tk.heap[0].key
		tk.keyMap.Delete(oldKey)
		tk.heap[0] = newKC
		tk.keyMap.Store(key, 0)
		tk.bubbleDown(0)
	}
}

// GetTopK는 상위 K개 키를 반환합니다.
func (tk *TopKHotKeys) GetTopK() []string {
	tk.mu.RLock()
	defer tk.mu.RUnlock()

	result := make([]string, len(tk.heap))
	for i, kc := range tk.heap {
		result[i] = kc.key
	}
	return result
}

func (tk *TopKHotKeys) bubbleUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if tk.heap[parent].count <= tk.heap[i].count {
			break
		}
		tk.swap(i, parent)
		i = parent
	}
}

func (tk *TopKHotKeys) bubbleDown(i int) {
	n := len(tk.heap)
	for {
		smallest := i
		left := 2*i + 1
		right := 2*i + 2

		if left < n && tk.heap[left].count < tk.heap[smallest].count {
			smallest = left
		}
		if right < n && tk.heap[right].count < tk.heap[smallest].count {
			smallest = right
		}
		if smallest == i {
			break
		}
		tk.swap(i, smallest)
		i = smallest
	}
}

func (tk *TopKHotKeys) swap(i, j int) {
	tk.heap[i], tk.heap[j] = tk.heap[j], tk.heap[i]
	tk.keyMap.Store(tk.heap[i].key, i)
	tk.keyMap.Store(tk.heap[j].key, j)
}
