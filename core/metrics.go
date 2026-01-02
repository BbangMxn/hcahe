// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Prometheus 메트릭을 구현합니다.
package core

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Metrics Collector: 메트릭 수집기
// =============================================================================

// MetricsCollector는 캐시 메트릭을 수집합니다.
type MetricsCollector struct {
	// 기본 카운터
	hits      uint64
	misses    uint64
	sets      uint64
	deletes   uint64
	evictions uint64

	// 지연 시간 (나노초)
	getTotalLatency uint64
	getCount        uint64
	setTotalLatency uint64
	setCount        uint64

	// 계층별 히트
	layerHits   []uint64
	layerMisses []uint64
	layerCount  int

	// 에러 카운터
	errors uint64

	// 서킷 브레이커 상태
	circuitOpen uint64

	mu sync.RWMutex
}

// NewMetricsCollector는 새로운 메트릭 수집기를 생성합니다.
func NewMetricsCollector(layerCount int) *MetricsCollector {
	return &MetricsCollector{
		layerHits:   make([]uint64, layerCount),
		layerMisses: make([]uint64, layerCount),
		layerCount:  layerCount,
	}
}

// RecordHit은 히트를 기록합니다.
func (m *MetricsCollector) RecordHit(layer int) {
	atomic.AddUint64(&m.hits, 1)
	if layer >= 0 && layer < m.layerCount {
		atomic.AddUint64(&m.layerHits[layer], 1)
	}
}

// RecordMiss는 미스를 기록합니다.
func (m *MetricsCollector) RecordMiss(layer int) {
	atomic.AddUint64(&m.misses, 1)
	if layer >= 0 && layer < m.layerCount {
		atomic.AddUint64(&m.layerMisses[layer], 1)
	}
}

// RecordSet은 저장을 기록합니다.
func (m *MetricsCollector) RecordSet() {
	atomic.AddUint64(&m.sets, 1)
}

// RecordDelete는 삭제를 기록합니다.
func (m *MetricsCollector) RecordDelete() {
	atomic.AddUint64(&m.deletes, 1)
}

// RecordEviction은 퇴거를 기록합니다.
func (m *MetricsCollector) RecordEviction() {
	atomic.AddUint64(&m.evictions, 1)
}

// RecordGetLatency는 Get 지연 시간을 기록합니다.
func (m *MetricsCollector) RecordGetLatency(d time.Duration) {
	atomic.AddUint64(&m.getTotalLatency, uint64(d.Nanoseconds()))
	atomic.AddUint64(&m.getCount, 1)
}

// RecordSetLatency는 Set 지연 시간을 기록합니다.
func (m *MetricsCollector) RecordSetLatency(d time.Duration) {
	atomic.AddUint64(&m.setTotalLatency, uint64(d.Nanoseconds()))
	atomic.AddUint64(&m.setCount, 1)
}

// RecordError는 에러를 기록합니다.
func (m *MetricsCollector) RecordError() {
	atomic.AddUint64(&m.errors, 1)
}

// RecordCircuitOpen은 서킷 오픈을 기록합니다.
func (m *MetricsCollector) RecordCircuitOpen() {
	atomic.AddUint64(&m.circuitOpen, 1)
}

// =============================================================================
// Prometheus 메트릭 포맷
// =============================================================================

// PrometheusMetrics는 Prometheus 형식의 메트릭을 반환합니다.
func (m *MetricsCollector) PrometheusMetrics() string {
	hits := atomic.LoadUint64(&m.hits)
	misses := atomic.LoadUint64(&m.misses)
	sets := atomic.LoadUint64(&m.sets)
	deletes := atomic.LoadUint64(&m.deletes)
	evictions := atomic.LoadUint64(&m.evictions)
	errors := atomic.LoadUint64(&m.errors)
	circuitOpen := atomic.LoadUint64(&m.circuitOpen)

	getTotalLatency := atomic.LoadUint64(&m.getTotalLatency)
	getCount := atomic.LoadUint64(&m.getCount)
	setTotalLatency := atomic.LoadUint64(&m.setTotalLatency)
	setCount := atomic.LoadUint64(&m.setCount)

	var avgGetLatency, avgSetLatency float64
	if getCount > 0 {
		avgGetLatency = float64(getTotalLatency) / float64(getCount) / 1e6 // ms
	}
	if setCount > 0 {
		avgSetLatency = float64(setTotalLatency) / float64(setCount) / 1e6 // ms
	}

	total := hits + misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	var sb []byte

	// 기본 메트릭
	sb = append(sb, "# HELP hcache_hits_total Total cache hits\n"...)
	sb = append(sb, "# TYPE hcache_hits_total counter\n"...)
	sb = append(sb, "hcache_hits_total "...)
	sb = append(sb, strconv.FormatUint(hits, 10)...)
	sb = append(sb, "\n"...)

	sb = append(sb, "# HELP hcache_misses_total Total cache misses\n"...)
	sb = append(sb, "# TYPE hcache_misses_total counter\n"...)
	sb = append(sb, "hcache_misses_total "...)
	sb = append(sb, strconv.FormatUint(misses, 10)...)
	sb = append(sb, "\n"...)

	sb = append(sb, "# HELP hcache_hit_rate Cache hit rate\n"...)
	sb = append(sb, "# TYPE hcache_hit_rate gauge\n"...)
	sb = append(sb, "hcache_hit_rate "...)
	sb = append(sb, strconv.FormatFloat(hitRate, 'f', 4, 64)...)
	sb = append(sb, "\n"...)

	sb = append(sb, "# HELP hcache_sets_total Total cache sets\n"...)
	sb = append(sb, "# TYPE hcache_sets_total counter\n"...)
	sb = append(sb, "hcache_sets_total "...)
	sb = append(sb, strconv.FormatUint(sets, 10)...)
	sb = append(sb, "\n"...)

	sb = append(sb, "# HELP hcache_deletes_total Total cache deletes\n"...)
	sb = append(sb, "# TYPE hcache_deletes_total counter\n"...)
	sb = append(sb, "hcache_deletes_total "...)
	sb = append(sb, strconv.FormatUint(deletes, 10)...)
	sb = append(sb, "\n"...)

	sb = append(sb, "# HELP hcache_evictions_total Total cache evictions\n"...)
	sb = append(sb, "# TYPE hcache_evictions_total counter\n"...)
	sb = append(sb, "hcache_evictions_total "...)
	sb = append(sb, strconv.FormatUint(evictions, 10)...)
	sb = append(sb, "\n"...)

	sb = append(sb, "# HELP hcache_errors_total Total errors\n"...)
	sb = append(sb, "# TYPE hcache_errors_total counter\n"...)
	sb = append(sb, "hcache_errors_total "...)
	sb = append(sb, strconv.FormatUint(errors, 10)...)
	sb = append(sb, "\n"...)

	sb = append(sb, "# HELP hcache_circuit_open_total Circuit breaker open events\n"...)
	sb = append(sb, "# TYPE hcache_circuit_open_total counter\n"...)
	sb = append(sb, "hcache_circuit_open_total "...)
	sb = append(sb, strconv.FormatUint(circuitOpen, 10)...)
	sb = append(sb, "\n"...)

	// 지연 시간
	sb = append(sb, "# HELP hcache_get_latency_ms Average get latency in milliseconds\n"...)
	sb = append(sb, "# TYPE hcache_get_latency_ms gauge\n"...)
	sb = append(sb, "hcache_get_latency_ms "...)
	sb = append(sb, strconv.FormatFloat(avgGetLatency, 'f', 4, 64)...)
	sb = append(sb, "\n"...)

	sb = append(sb, "# HELP hcache_set_latency_ms Average set latency in milliseconds\n"...)
	sb = append(sb, "# TYPE hcache_set_latency_ms gauge\n"...)
	sb = append(sb, "hcache_set_latency_ms "...)
	sb = append(sb, strconv.FormatFloat(avgSetLatency, 'f', 4, 64)...)
	sb = append(sb, "\n"...)

	// 계층별 히트
	sb = append(sb, "# HELP hcache_layer_hits_total Hits per layer\n"...)
	sb = append(sb, "# TYPE hcache_layer_hits_total counter\n"...)
	for i := 0; i < m.layerCount; i++ {
		sb = append(sb, "hcache_layer_hits_total{layer=\""...)
		sb = append(sb, strconv.Itoa(i)...)
		sb = append(sb, "\"} "...)
		sb = append(sb, strconv.FormatUint(atomic.LoadUint64(&m.layerHits[i]), 10)...)
		sb = append(sb, "\n"...)
	}

	sb = append(sb, "# HELP hcache_layer_misses_total Misses per layer\n"...)
	sb = append(sb, "# TYPE hcache_layer_misses_total counter\n"...)
	for i := 0; i < m.layerCount; i++ {
		sb = append(sb, "hcache_layer_misses_total{layer=\""...)
		sb = append(sb, strconv.Itoa(i)...)
		sb = append(sb, "\"} "...)
		sb = append(sb, strconv.FormatUint(atomic.LoadUint64(&m.layerMisses[i]), 10)...)
		sb = append(sb, "\n"...)
	}

	return string(sb)
}

// Snapshot은 메트릭 스냅샷을 반환합니다.
func (m *MetricsCollector) Snapshot() *MetricsSnapshot {
	getCount := atomic.LoadUint64(&m.getCount)
	setCount := atomic.LoadUint64(&m.setCount)

	var avgGetLatency, avgSetLatency time.Duration
	if getCount > 0 {
		avgGetLatency = time.Duration(atomic.LoadUint64(&m.getTotalLatency) / getCount)
	}
	if setCount > 0 {
		avgSetLatency = time.Duration(atomic.LoadUint64(&m.setTotalLatency) / setCount)
	}

	layerHits := make([]uint64, m.layerCount)
	layerMisses := make([]uint64, m.layerCount)
	for i := 0; i < m.layerCount; i++ {
		layerHits[i] = atomic.LoadUint64(&m.layerHits[i])
		layerMisses[i] = atomic.LoadUint64(&m.layerMisses[i])
	}

	return &MetricsSnapshot{
		Hits:          atomic.LoadUint64(&m.hits),
		Misses:        atomic.LoadUint64(&m.misses),
		Sets:          atomic.LoadUint64(&m.sets),
		Deletes:       atomic.LoadUint64(&m.deletes),
		Evictions:     atomic.LoadUint64(&m.evictions),
		Errors:        atomic.LoadUint64(&m.errors),
		CircuitOpen:   atomic.LoadUint64(&m.circuitOpen),
		AvgGetLatency: avgGetLatency,
		AvgSetLatency: avgSetLatency,
		LayerHits:     layerHits,
		LayerMisses:   layerMisses,
	}
}

// Reset은 메트릭을 초기화합니다.
func (m *MetricsCollector) Reset() {
	atomic.StoreUint64(&m.hits, 0)
	atomic.StoreUint64(&m.misses, 0)
	atomic.StoreUint64(&m.sets, 0)
	atomic.StoreUint64(&m.deletes, 0)
	atomic.StoreUint64(&m.evictions, 0)
	atomic.StoreUint64(&m.errors, 0)
	atomic.StoreUint64(&m.circuitOpen, 0)
	atomic.StoreUint64(&m.getTotalLatency, 0)
	atomic.StoreUint64(&m.getCount, 0)
	atomic.StoreUint64(&m.setTotalLatency, 0)
	atomic.StoreUint64(&m.setCount, 0)

	for i := 0; i < m.layerCount; i++ {
		atomic.StoreUint64(&m.layerHits[i], 0)
		atomic.StoreUint64(&m.layerMisses[i], 0)
	}
}

// MetricsSnapshot은 메트릭 스냅샷입니다.
type MetricsSnapshot struct {
	Hits          uint64
	Misses        uint64
	Sets          uint64
	Deletes       uint64
	Evictions     uint64
	Errors        uint64
	CircuitOpen   uint64
	AvgGetLatency time.Duration
	AvgSetLatency time.Duration
	LayerHits     []uint64
	LayerMisses   []uint64
}

// HitRate는 히트율을 반환합니다.
func (s *MetricsSnapshot) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// =============================================================================
// HTTP Handler
// =============================================================================

// MetricsHandler는 Prometheus 메트릭을 제공하는 HTTP 핸들러입니다.
type MetricsHandler struct {
	collector *MetricsCollector
	cache     *Cache
}

// NewMetricsHandler는 새로운 메트릭 핸들러를 생성합니다.
func NewMetricsHandler(cache *Cache, collector *MetricsCollector) *MetricsHandler {
	return &MetricsHandler{
		collector: collector,
		cache:     cache,
	}
}

// ServeHTTP는 Prometheus 메트릭을 응답합니다.
func (h *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 기본 메트릭
	metrics := h.collector.PrometheusMetrics()

	// 캐시 크기 메트릭 추가
	ctx := context.Background()
	stats, err := h.cache.Stats(ctx)
	if err == nil {
		metrics += "# HELP hcache_size Current cache size\n"
		metrics += "# TYPE hcache_size gauge\n"
		for i, s := range stats {
			metrics += "hcache_size{layer=\"" + strconv.Itoa(i) + "\",name=\"" + s.Name + "\"} " + strconv.FormatInt(s.Size, 10) + "\n"
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(metrics))
}
