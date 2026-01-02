// Package metrics는 캐시 메트릭 수집을 구현합니다.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/BbangMxn/hcahe/core"
)

// =============================================================================
// MetricsCollector: 메트릭 수집기
// =============================================================================
// MetricsCollector는 캐시의 다양한 메트릭을 수집하고 제공합니다.
// Prometheus, StatsD 등과 통합하거나 직접 조회할 수 있습니다.
// =============================================================================

// Collector는 메트릭 수집기입니다.
type Collector struct {
	// 전역 메트릭
	totalGets      uint64
	totalSets      uint64
	totalDeletes   uint64
	totalHits      uint64
	totalMisses    uint64
	totalEvictions uint64

	// 지연 시간 (나노초)
	getLatencySum    int64
	setLatencySum    int64
	deleteLatencySum int64

	// 계층별 메트릭
	layerHits   map[int]*uint64
	layerMisses map[int]*uint64

	// 크기 메트릭
	totalSize   int64
	totalMemory int64

	// 히스토그램 (지연 시간 분포)
	getLatencyBuckets []int64 // [<1ms, 1-5ms, 5-10ms, 10-50ms, 50-100ms, >100ms]
	setLatencyBuckets []int64

	// 시계열 (최근 N분간 데이터)
	timeSeries *TimeSeries

	mu sync.RWMutex
}

// =============================================================================
// Collector 생성자
// =============================================================================

// New는 새로운 메트릭 수집기를 생성합니다.
func New() *Collector {
	c := &Collector{
		layerHits:         make(map[int]*uint64),
		layerMisses:       make(map[int]*uint64),
		getLatencyBuckets: make([]int64, 6),
		setLatencyBuckets: make([]int64, 6),
		timeSeries:        NewTimeSeries(60), // 60분
	}
	return c
}

// =============================================================================
// Collector 메트릭 기록
// =============================================================================

// RecordGet은 Get 호출을 기록합니다.
func (c *Collector) RecordGet(layer int, hit bool, latencyNs int64) {
	atomic.AddUint64(&c.totalGets, 1)
	atomic.AddInt64(&c.getLatencySum, latencyNs)

	if hit {
		atomic.AddUint64(&c.totalHits, 1)
		c.incrementLayerHits(layer)
	} else {
		atomic.AddUint64(&c.totalMisses, 1)
		c.incrementLayerMisses(layer)
	}

	// 히스토그램 업데이트
	c.recordLatencyBucket(c.getLatencyBuckets, latencyNs)

	// 시계열 업데이트
	c.timeSeries.RecordGet(hit)
}

// RecordSet은 Set 호출을 기록합니다.
func (c *Collector) RecordSet(size int, latencyNs int64) {
	atomic.AddUint64(&c.totalSets, 1)
	atomic.AddInt64(&c.setLatencySum, latencyNs)
	atomic.AddInt64(&c.totalSize, int64(size))

	c.recordLatencyBucket(c.setLatencyBuckets, latencyNs)
	c.timeSeries.RecordSet()
}

// RecordDelete는 Delete 호출을 기록합니다.
func (c *Collector) RecordDelete(latencyNs int64) {
	atomic.AddUint64(&c.totalDeletes, 1)
	atomic.AddInt64(&c.deleteLatencySum, latencyNs)
}

// RecordEviction은 퇴거를 기록합니다.
func (c *Collector) RecordEviction() {
	atomic.AddUint64(&c.totalEvictions, 1)
	c.timeSeries.RecordEviction()
}

// =============================================================================
// Collector 내부 헬퍼
// =============================================================================

func (c *Collector) incrementLayerHits(layer int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.layerHits[layer]; !exists {
		var zero uint64
		c.layerHits[layer] = &zero
	}
	atomic.AddUint64(c.layerHits[layer], 1)
}

func (c *Collector) incrementLayerMisses(layer int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.layerMisses[layer]; !exists {
		var zero uint64
		c.layerMisses[layer] = &zero
	}
	atomic.AddUint64(c.layerMisses[layer], 1)
}

func (c *Collector) recordLatencyBucket(buckets []int64, latencyNs int64) {
	latencyMs := latencyNs / 1_000_000

	var idx int
	switch {
	case latencyMs < 1:
		idx = 0
	case latencyMs < 5:
		idx = 1
	case latencyMs < 10:
		idx = 2
	case latencyMs < 50:
		idx = 3
	case latencyMs < 100:
		idx = 4
	default:
		idx = 5
	}

	atomic.AddInt64(&buckets[idx], 1)
}

// =============================================================================
// Collector 메트릭 조회
// =============================================================================

// Stats는 현재 메트릭을 반환합니다.
type Stats struct {
	// 기본 통계
	TotalGets      uint64  `json:"total_gets"`
	TotalSets      uint64  `json:"total_sets"`
	TotalDeletes   uint64  `json:"total_deletes"`
	TotalHits      uint64  `json:"total_hits"`
	TotalMisses    uint64  `json:"total_misses"`
	TotalEvictions uint64  `json:"total_evictions"`
	HitRate        float64 `json:"hit_rate"`

	// 지연 시간 (밀리초)
	AvgGetLatencyMs    float64 `json:"avg_get_latency_ms"`
	AvgSetLatencyMs    float64 `json:"avg_set_latency_ms"`
	AvgDeleteLatencyMs float64 `json:"avg_delete_latency_ms"`

	// 계층별 히트율
	LayerHitRates map[int]float64 `json:"layer_hit_rates"`

	// 지연 시간 분포 (퍼센트)
	GetLatencyDistribution []float64 `json:"get_latency_distribution"`
	SetLatencyDistribution []float64 `json:"set_latency_distribution"`

	// 크기
	TotalSize   int64 `json:"total_size"`
	TotalMemory int64 `json:"total_memory"`
}

// Stats는 현재 메트릭을 반환합니다.
func (c *Collector) Stats() *Stats {
	totalGets := atomic.LoadUint64(&c.totalGets)
	totalSets := atomic.LoadUint64(&c.totalSets)
	totalDeletes := atomic.LoadUint64(&c.totalDeletes)
	totalHits := atomic.LoadUint64(&c.totalHits)
	totalMisses := atomic.LoadUint64(&c.totalMisses)

	// 히트율 계산
	var hitRate float64
	total := totalHits + totalMisses
	if total > 0 {
		hitRate = float64(totalHits) / float64(total)
	}

	// 평균 지연 시간 계산
	var avgGetLatency, avgSetLatency, avgDeleteLatency float64
	if totalGets > 0 {
		avgGetLatency = float64(atomic.LoadInt64(&c.getLatencySum)) / float64(totalGets) / 1_000_000
	}
	if totalSets > 0 {
		avgSetLatency = float64(atomic.LoadInt64(&c.setLatencySum)) / float64(totalSets) / 1_000_000
	}
	if totalDeletes > 0 {
		avgDeleteLatency = float64(atomic.LoadInt64(&c.deleteLatencySum)) / float64(totalDeletes) / 1_000_000
	}

	// 계층별 히트율
	layerHitRates := make(map[int]float64)
	c.mu.RLock()
	for layer := range c.layerHits {
		hits := atomic.LoadUint64(c.layerHits[layer])
		misses := uint64(0)
		if c.layerMisses[layer] != nil {
			misses = atomic.LoadUint64(c.layerMisses[layer])
		}
		total := hits + misses
		if total > 0 {
			layerHitRates[layer] = float64(hits) / float64(total)
		}
	}
	c.mu.RUnlock()

	// 지연 시간 분포
	getLatencyDist := c.calculateDistribution(c.getLatencyBuckets)
	setLatencyDist := c.calculateDistribution(c.setLatencyBuckets)

	return &Stats{
		TotalGets:              totalGets,
		TotalSets:              totalSets,
		TotalDeletes:           totalDeletes,
		TotalHits:              totalHits,
		TotalMisses:            totalMisses,
		TotalEvictions:         atomic.LoadUint64(&c.totalEvictions),
		HitRate:                hitRate,
		AvgGetLatencyMs:        avgGetLatency,
		AvgSetLatencyMs:        avgSetLatency,
		AvgDeleteLatencyMs:     avgDeleteLatency,
		LayerHitRates:          layerHitRates,
		GetLatencyDistribution: getLatencyDist,
		SetLatencyDistribution: setLatencyDist,
		TotalSize:              atomic.LoadInt64(&c.totalSize),
		TotalMemory:            atomic.LoadInt64(&c.totalMemory),
	}
}

func (c *Collector) calculateDistribution(buckets []int64) []float64 {
	var total int64
	for i := range buckets {
		total += atomic.LoadInt64(&buckets[i])
	}

	if total == 0 {
		return make([]float64, len(buckets))
	}

	dist := make([]float64, len(buckets))
	for i := range buckets {
		dist[i] = float64(atomic.LoadInt64(&buckets[i])) / float64(total) * 100
	}
	return dist
}

// Reset은 모든 메트릭을 초기화합니다.
func (c *Collector) Reset() {
	atomic.StoreUint64(&c.totalGets, 0)
	atomic.StoreUint64(&c.totalSets, 0)
	atomic.StoreUint64(&c.totalDeletes, 0)
	atomic.StoreUint64(&c.totalHits, 0)
	atomic.StoreUint64(&c.totalMisses, 0)
	atomic.StoreUint64(&c.totalEvictions, 0)
	atomic.StoreInt64(&c.getLatencySum, 0)
	atomic.StoreInt64(&c.setLatencySum, 0)
	atomic.StoreInt64(&c.deleteLatencySum, 0)

	for i := range c.getLatencyBuckets {
		atomic.StoreInt64(&c.getLatencyBuckets[i], 0)
	}
	for i := range c.setLatencyBuckets {
		atomic.StoreInt64(&c.setLatencyBuckets[i], 0)
	}

	c.mu.Lock()
	c.layerHits = make(map[int]*uint64)
	c.layerMisses = make(map[int]*uint64)
	c.mu.Unlock()
}

// =============================================================================
// TimeSeries: 시계열 데이터
// =============================================================================

// TimeSeries는 시계열 메트릭을 저장합니다.
type TimeSeries struct {
	windowMinutes int
	buckets       []TimeSeriesBucket
	currentIdx    int
	lastRotation  time.Time
	mu            sync.Mutex
}

// TimeSeriesBucket은 1분 단위 버킷입니다.
type TimeSeriesBucket struct {
	Timestamp time.Time
	Gets      uint64
	Hits      uint64
	Sets      uint64
	Evictions uint64
}

// NewTimeSeries는 새로운 시계열을 생성합니다.
func NewTimeSeries(windowMinutes int) *TimeSeries {
	ts := &TimeSeries{
		windowMinutes: windowMinutes,
		buckets:       make([]TimeSeriesBucket, windowMinutes),
		lastRotation:  time.Now().Truncate(time.Minute),
	}
	return ts
}

func (ts *TimeSeries) maybeRotate() {
	now := time.Now().Truncate(time.Minute)
	if now.After(ts.lastRotation) {
		// 새 분으로 이동
		minutesPassed := int(now.Sub(ts.lastRotation).Minutes())
		for i := 0; i < minutesPassed && i < ts.windowMinutes; i++ {
			ts.currentIdx = (ts.currentIdx + 1) % ts.windowMinutes
			ts.buckets[ts.currentIdx] = TimeSeriesBucket{Timestamp: now}
		}
		ts.lastRotation = now
	}
}

func (ts *TimeSeries) RecordGet(hit bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.maybeRotate()
	ts.buckets[ts.currentIdx].Gets++
	if hit {
		ts.buckets[ts.currentIdx].Hits++
	}
}

func (ts *TimeSeries) RecordSet() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.maybeRotate()
	ts.buckets[ts.currentIdx].Sets++
}

func (ts *TimeSeries) RecordEviction() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	ts.maybeRotate()
	ts.buckets[ts.currentIdx].Evictions++
}

// GetHistory는 최근 N분간의 데이터를 반환합니다.
func (ts *TimeSeries) GetHistory(minutes int) []TimeSeriesBucket {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if minutes > ts.windowMinutes {
		minutes = ts.windowMinutes
	}

	result := make([]TimeSeriesBucket, minutes)
	for i := 0; i < minutes; i++ {
		idx := (ts.currentIdx - i + ts.windowMinutes) % ts.windowMinutes
		result[i] = ts.buckets[idx]
	}

	return result
}

// =============================================================================
// MetricsPlugin: 메트릭 수집 플러그인
// =============================================================================

// MetricsPlugin은 메트릭을 수집하는 플러그인입니다.
type MetricsPlugin struct {
	collector *Collector
}

// NewMetricsPlugin은 메트릭 플러그인을 생성합니다.
func NewMetricsPlugin(collector *Collector) *MetricsPlugin {
	return &MetricsPlugin{collector: collector}
}

func (p *MetricsPlugin) Name() string { return "metrics" }

func (p *MetricsPlugin) OnGet(key string, entry *core.Entry, layer int, hit bool) {
	// 지연 시간은 Layer에서 측정하므로 여기서는 0으로 기록
	p.collector.RecordGet(layer, hit, 0)
}

func (p *MetricsPlugin) OnSet(key string, entry *core.Entry, layers []int) {
	if entry != nil {
		p.collector.RecordSet(entry.Size, 0)
	}
}

func (p *MetricsPlugin) OnDelete(key string, layers []int) {
	p.collector.RecordDelete(0)
}

func (p *MetricsPlugin) OnEviction(key string, layer int, reason string) {
	p.collector.RecordEviction()
}

func (p *MetricsPlugin) OnPromotion(key string, from, to int) {}
func (p *MetricsPlugin) OnDemotion(key string, from, to int)  {}
