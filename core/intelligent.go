// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 지능형 캐싱 기능을 정의합니다.
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// PersistMode: 영구 저장 모드
// =============================================================================
// 캐싱과 영구 저장을 분리하여 사용자가 원하는 방식으로 조합할 수 있습니다.
// =============================================================================

// PersistMode는 데이터 영구 저장 모드를 나타냅니다.
type PersistMode int

const (
	// CacheOnly는 캐시에만 저장하고 영구 저장하지 않습니다.
	// 가장 빠르지만 재시작 시 데이터가 손실됩니다.
	CacheOnly PersistMode = iota

	// WriteThrough는 캐시와 영구 저장소에 동시에 저장합니다.
	// 데이터 일관성이 보장되지만 쓰기 지연이 증가합니다.
	WriteThrough

	// WriteBehind는 캐시에 먼저 저장하고 비동기로 영구 저장합니다.
	// 쓰기 성능이 좋지만 장애 시 데이터 손실 가능성이 있습니다.
	WriteBehind

	// ReadOnly는 읽기만 허용하고 쓰기를 차단합니다.
	// 공유 캐시나 읽기 복제본에 사용합니다.
	ReadOnly
)

// String은 PersistMode의 문자열 표현을 반환합니다.
func (m PersistMode) String() string {
	switch m {
	case CacheOnly:
		return "cache_only"
	case WriteThrough:
		return "write_through"
	case WriteBehind:
		return "write_behind"
	case ReadOnly:
		return "read_only"
	default:
		return "unknown"
	}
}

// =============================================================================
// IntelligentOptions: 지능형 캐싱 옵션
// =============================================================================

// IntelligentOptions는 지능형 캐싱 기능의 설정입니다.
type IntelligentOptions struct {
	// === 예측 캐싱 ===

	// PredictiveCaching은 예측 캐싱 활성화 여부입니다.
	PredictiveCaching bool

	// PrefetchThreshold는 프리페치를 위한 최소 접근 횟수입니다.
	// 이 횟수 이상 접근된 패턴은 연관 데이터를 미리 로드합니다.
	PrefetchThreshold int

	// PatternWindowSize는 패턴 분석을 위한 시간 윈도우입니다.
	PatternWindowSize time.Duration

	// MaxPrefetchItems는 한 번에 프리페치할 최대 항목 수입니다.
	MaxPrefetchItems int

	// === 적응형 TTL ===

	// AdaptiveTTL은 적응형 TTL 활성화 여부입니다.
	AdaptiveTTL bool

	// MinTTL은 적응형 TTL의 최소값입니다.
	MinTTL time.Duration

	// MaxTTL은 적응형 TTL의 최대값입니다.
	MaxTTL time.Duration

	// TTLAdjustInterval은 TTL 조정 주기입니다.
	TTLAdjustInterval time.Duration

	// === 스마트 무효화 ===

	// DependencyTracking은 의존성 추적 활성화 여부입니다.
	DependencyTracking bool

	// TagSupport는 태그 기반 무효화 활성화 여부입니다.
	TagSupport bool

	// MaxDependencyDepth는 의존성 그래프의 최대 깊이입니다.
	MaxDependencyDepth int

	// === 메모리 최적화 ===

	// ObjectPooling은 객체 풀링 활성화 여부입니다.
	ObjectPooling bool

	// PoolSize는 객체 풀의 크기입니다.
	PoolSize int

	// ZeroCopy는 Zero-Copy 최적화 활성화 여부입니다.
	ZeroCopy bool

	// === 저장 모드 ===

	// PersistMode는 영구 저장 모드입니다.
	PersistMode PersistMode

	// PersistLayer는 영구 저장에 사용할 계층 인덱스입니다.
	// -1이면 마지막 계층을 사용합니다.
	PersistLayer int

	// PersistBatchSize는 WriteBehind 모드에서 배치 크기입니다.
	PersistBatchSize int

	// PersistFlushInterval은 WriteBehind 모드에서 플러시 주기입니다.
	PersistFlushInterval time.Duration
}

// DefaultIntelligentOptions는 기본 지능형 옵션을 반환합니다.
func DefaultIntelligentOptions() *IntelligentOptions {
	return &IntelligentOptions{
		// 예측 캐싱: 기본 비활성화
		PredictiveCaching: false,
		PrefetchThreshold: 3,
		PatternWindowSize: 5 * time.Minute,
		MaxPrefetchItems:  10,

		// 적응형 TTL: 기본 비활성화
		AdaptiveTTL:       false,
		MinTTL:            1 * time.Minute,
		MaxTTL:            24 * time.Hour,
		TTLAdjustInterval: 1 * time.Minute,

		// 스마트 무효화: 태그만 기본 활성화
		DependencyTracking: false,
		TagSupport:         true,
		MaxDependencyDepth: 5,

		// 메모리 최적화: 객체 풀링만 기본 활성화
		ObjectPooling: true,
		PoolSize:      1000,
		ZeroCopy:      false,

		// 저장 모드: 기본 CacheOnly
		PersistMode:          CacheOnly,
		PersistLayer:         -1,
		PersistBatchSize:     100,
		PersistFlushInterval: 100 * time.Millisecond,
	}
}

// =============================================================================
// Intelligent Options Functional Pattern
// =============================================================================

// IntelligentOption은 지능형 옵션을 설정하는 함수입니다.
type IntelligentOption func(*IntelligentOptions)

// WithPredictiveCaching은 예측 캐싱을 설정합니다.
func WithPredictiveCaching(threshold int, windowSize time.Duration) IntelligentOption {
	return func(o *IntelligentOptions) {
		o.PredictiveCaching = true
		o.PrefetchThreshold = threshold
		o.PatternWindowSize = windowSize
	}
}

// WithoutPredictiveCaching은 예측 캐싱을 비활성화합니다.
func WithoutPredictiveCaching() IntelligentOption {
	return func(o *IntelligentOptions) {
		o.PredictiveCaching = false
	}
}

// WithAdaptiveTTL은 적응형 TTL을 설정합니다.
func WithAdaptiveTTL(minTTL, maxTTL time.Duration) IntelligentOption {
	return func(o *IntelligentOptions) {
		o.AdaptiveTTL = true
		o.MinTTL = minTTL
		o.MaxTTL = maxTTL
	}
}

// WithoutAdaptiveTTL은 적응형 TTL을 비활성화합니다.
func WithoutAdaptiveTTL() IntelligentOption {
	return func(o *IntelligentOptions) {
		o.AdaptiveTTL = false
	}
}

// WithDependencyTracking은 의존성 추적을 활성화합니다.
func WithDependencyTracking(maxDepth int) IntelligentOption {
	return func(o *IntelligentOptions) {
		o.DependencyTracking = true
		o.MaxDependencyDepth = maxDepth
	}
}

// WithTagSupport는 태그 기반 무효화를 설정합니다.
func WithTagSupport(enabled bool) IntelligentOption {
	return func(o *IntelligentOptions) {
		o.TagSupport = enabled
	}
}

// WithObjectPooling은 객체 풀링을 설정합니다.
func WithObjectPooling(poolSize int) IntelligentOption {
	return func(o *IntelligentOptions) {
		o.ObjectPooling = true
		o.PoolSize = poolSize
	}
}

// WithZeroCopy는 Zero-Copy 최적화를 활성화합니다.
func WithZeroCopy() IntelligentOption {
	return func(o *IntelligentOptions) {
		o.ZeroCopy = true
	}
}

// WithPersistMode는 영구 저장 모드를 설정합니다.
func WithPersistMode(mode PersistMode) IntelligentOption {
	return func(o *IntelligentOptions) {
		o.PersistMode = mode
	}
}

// WithPersistLayer는 영구 저장 계층을 지정합니다.
func WithPersistLayer(layerIndex int) IntelligentOption {
	return func(o *IntelligentOptions) {
		o.PersistLayer = layerIndex
	}
}

// WithWriteBehind는 WriteBehind 모드를 설정합니다.
func WithWriteBehind(batchSize int, flushInterval time.Duration) IntelligentOption {
	return func(o *IntelligentOptions) {
		o.PersistMode = WriteBehind
		o.PersistBatchSize = batchSize
		o.PersistFlushInterval = flushInterval
	}
}

// =============================================================================
// AccessPattern: 접근 패턴 추적
// =============================================================================

// AccessPattern은 키별 접근 패턴을 추적합니다.
type AccessPattern struct {
	Key           string
	AccessCount   uint64
	LastAccess    time.Time
	AvgInterval   time.Duration
	AccessHistory []time.Time
	mu            sync.RWMutex
}

// NewAccessPattern은 새로운 접근 패턴을 생성합니다.
func NewAccessPattern(key string) *AccessPattern {
	return &AccessPattern{
		Key:           key,
		LastAccess:    time.Now(),
		AccessHistory: make([]time.Time, 0, 100),
	}
}

// Record는 접근을 기록합니다.
func (p *AccessPattern) Record() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	atomic.AddUint64(&p.AccessCount, 1)

	// 평균 간격 계산
	if !p.LastAccess.IsZero() {
		interval := now.Sub(p.LastAccess)
		if p.AvgInterval == 0 {
			p.AvgInterval = interval
		} else {
			// 이동 평균
			p.AvgInterval = (p.AvgInterval*9 + interval) / 10
		}
	}

	p.LastAccess = now

	// 히스토리 유지 (최대 100개)
	p.AccessHistory = append(p.AccessHistory, now)
	if len(p.AccessHistory) > 100 {
		p.AccessHistory = p.AccessHistory[1:]
	}
}

// GetAccessCount는 접근 횟수를 반환합니다.
func (p *AccessPattern) GetAccessCount() uint64 {
	return atomic.LoadUint64(&p.AccessCount)
}

// GetAvgInterval은 평균 접근 간격을 반환합니다.
func (p *AccessPattern) GetAvgInterval() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.AvgInterval
}

// =============================================================================
// PatternTracker: 접근 패턴 추적기
// =============================================================================

// PatternTracker는 전역 접근 패턴을 추적합니다.
type PatternTracker struct {
	patterns    map[string]*AccessPattern
	sequences   []string // 최근 접근 순서
	maxSequence int
	mu          sync.RWMutex
}

// NewPatternTracker는 새로운 패턴 추적기를 생성합니다.
func NewPatternTracker(maxSequence int) *PatternTracker {
	return &PatternTracker{
		patterns:    make(map[string]*AccessPattern),
		sequences:   make([]string, 0, maxSequence),
		maxSequence: maxSequence,
	}
}

// RecordAccess는 접근을 기록하고 연관 키를 반환합니다.
func (t *PatternTracker) RecordAccess(key string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 패턴 기록
	pattern, exists := t.patterns[key]
	if !exists {
		pattern = NewAccessPattern(key)
		t.patterns[key] = pattern
	}
	pattern.Record()

	// 시퀀스에 추가
	t.sequences = append(t.sequences, key)
	if len(t.sequences) > t.maxSequence {
		t.sequences = t.sequences[1:]
	}

	// 연관 키 찾기 (같이 자주 조회되는 키)
	return t.findRelatedKeys(key)
}

// findRelatedKeys는 연관된 키들을 찾습니다.
func (t *PatternTracker) findRelatedKeys(key string) []string {
	// 최근 시퀀스에서 key 다음에 자주 나오는 키 찾기
	nextKeys := make(map[string]int)

	for i := 0; i < len(t.sequences)-1; i++ {
		if t.sequences[i] == key {
			nextKey := t.sequences[i+1]
			if nextKey != key {
				nextKeys[nextKey]++
			}
		}
	}

	// 빈도순 정렬
	type keyFreq struct {
		key  string
		freq int
	}
	freqs := make([]keyFreq, 0, len(nextKeys))
	for k, f := range nextKeys {
		freqs = append(freqs, keyFreq{k, f})
	}

	// 상위 5개 반환
	result := make([]string, 0, 5)
	for _, kf := range freqs {
		if kf.freq >= 2 { // 최소 2번 이상
			result = append(result, kf.key)
			if len(result) >= 5 {
				break
			}
		}
	}

	return result
}

// GetPattern은 특정 키의 패턴을 반환합니다.
func (t *PatternTracker) GetPattern(key string) *AccessPattern {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.patterns[key]
}

// =============================================================================
// AdaptiveTTLManager: 적응형 TTL 관리자
// =============================================================================

// AdaptiveTTLManager는 데이터 변경 빈도에 따라 TTL을 자동 조절합니다.
type AdaptiveTTLManager struct {
	options     *IntelligentOptions
	updateStats map[string]*UpdateStats
	mu          sync.RWMutex
}

// UpdateStats는 키별 업데이트 통계입니다.
type UpdateStats struct {
	UpdateCount    uint64
	LastUpdate     time.Time
	AvgUpdateGap   time.Duration
	StabilityScore float64 // 0.0 ~ 1.0 (높을수록 안정적)
}

// NewAdaptiveTTLManager는 새로운 적응형 TTL 관리자를 생성합니다.
func NewAdaptiveTTLManager(options *IntelligentOptions) *AdaptiveTTLManager {
	return &AdaptiveTTLManager{
		options:     options,
		updateStats: make(map[string]*UpdateStats),
	}
}

// RecordUpdate는 업데이트를 기록합니다.
func (m *AdaptiveTTLManager) RecordUpdate(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats, exists := m.updateStats[key]
	if !exists {
		stats = &UpdateStats{
			LastUpdate:     time.Now(),
			StabilityScore: 0.5,
		}
		m.updateStats[key] = stats
	}

	now := time.Now()
	if !stats.LastUpdate.IsZero() {
		gap := now.Sub(stats.LastUpdate)

		// 이동 평균으로 업데이트 간격 계산
		if stats.AvgUpdateGap == 0 {
			stats.AvgUpdateGap = gap
		} else {
			stats.AvgUpdateGap = (stats.AvgUpdateGap*7 + gap) / 8
		}

		// 안정성 점수 조정
		// 업데이트 간격이 길수록 안정적
		if gap > stats.AvgUpdateGap {
			stats.StabilityScore = min(1.0, stats.StabilityScore+0.1)
		} else {
			stats.StabilityScore = max(0.0, stats.StabilityScore-0.1)
		}
	}

	stats.UpdateCount++
	stats.LastUpdate = now
}

// CalculateTTL은 키에 대한 적응형 TTL을 계산합니다.
func (m *AdaptiveTTLManager) CalculateTTL(key string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats, exists := m.updateStats[key]
	if !exists {
		// 통계 없으면 중간값
		return (m.options.MinTTL + m.options.MaxTTL) / 2
	}

	// 안정성 점수에 따라 TTL 결정
	// 안정적일수록 긴 TTL
	ttlRange := m.options.MaxTTL - m.options.MinTTL
	ttl := m.options.MinTTL + time.Duration(float64(ttlRange)*stats.StabilityScore)

	return ttl
}

// =============================================================================
// DependencyGraph: 의존성 그래프
// =============================================================================

// DependencyGraph는 캐시 키 간의 의존성을 추적합니다.
type DependencyGraph struct {
	// dependencies[A] = [B, C] 는 A가 B, C에 의존함을 의미
	// B나 C가 무효화되면 A도 무효화
	dependencies map[string][]string

	// dependents[B] = [A, D] 는 A, D가 B에 의존함을 의미
	// B가 무효화되면 A, D도 무효화
	dependents map[string][]string

	maxDepth int
	mu       sync.RWMutex
}

// NewDependencyGraph는 새로운 의존성 그래프를 생성합니다.
func NewDependencyGraph(maxDepth int) *DependencyGraph {
	return &DependencyGraph{
		dependencies: make(map[string][]string),
		dependents:   make(map[string][]string),
		maxDepth:     maxDepth,
	}
}

// AddDependency는 의존성을 추가합니다.
// key가 dependsOn에 의존함을 등록합니다.
func (g *DependencyGraph) AddDependency(key string, dependsOn ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, dep := range dependsOn {
		// 순환 의존성 체크
		if g.hasCyclicDependency(dep, key, 0) {
			continue
		}

		g.dependencies[key] = appendUnique(g.dependencies[key], dep)
		g.dependents[dep] = appendUnique(g.dependents[dep], key)
	}
}

// RemoveDependency는 의존성을 제거합니다.
func (g *DependencyGraph) RemoveDependency(key string, dependsOn ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, dep := range dependsOn {
		g.dependencies[key] = removeFromSlice(g.dependencies[key], dep)
		g.dependents[dep] = removeFromSlice(g.dependents[dep], key)
	}
}

// GetDependents는 key에 의존하는 모든 키를 반환합니다 (연쇄적으로).
func (g *DependencyGraph) GetDependents(key string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	visited := make(map[string]bool)
	result := make([]string, 0)
	g.collectDependents(key, visited, &result, 0)

	return result
}

// collectDependents는 재귀적으로 의존 키를 수집합니다.
func (g *DependencyGraph) collectDependents(key string, visited map[string]bool, result *[]string, depth int) {
	if depth >= g.maxDepth {
		return
	}

	deps := g.dependents[key]
	for _, dep := range deps {
		if !visited[dep] {
			visited[dep] = true
			*result = append(*result, dep)
			g.collectDependents(dep, visited, result, depth+1)
		}
	}
}

// hasCyclicDependency는 순환 의존성을 체크합니다.
func (g *DependencyGraph) hasCyclicDependency(from, to string, depth int) bool {
	if depth >= g.maxDepth {
		return false
	}
	if from == to {
		return true
	}

	deps := g.dependencies[from]
	for _, dep := range deps {
		if g.hasCyclicDependency(dep, to, depth+1) {
			return true
		}
	}

	return false
}

// Clear는 특정 키의 모든 의존성을 제거합니다.
func (g *DependencyGraph) Clear(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// 이 키가 의존하는 것들에서 제거
	for _, dep := range g.dependencies[key] {
		g.dependents[dep] = removeFromSlice(g.dependents[dep], key)
	}
	delete(g.dependencies, key)

	// 이 키에 의존하는 것들에서 제거
	for _, dep := range g.dependents[key] {
		g.dependencies[dep] = removeFromSlice(g.dependencies[dep], key)
	}
	delete(g.dependents, key)
}

// =============================================================================
// TagManager: 태그 기반 무효화 관리자
// =============================================================================

// TagManager는 태그 기반으로 캐시 키를 그룹화하고 무효화합니다.
type TagManager struct {
	// tagToKeys[tag] = [key1, key2, ...]
	tagToKeys map[string][]string

	// keyToTags[key] = [tag1, tag2, ...]
	keyToTags map[string][]string

	mu sync.RWMutex
}

// NewTagManager는 새로운 태그 관리자를 생성합니다.
func NewTagManager() *TagManager {
	return &TagManager{
		tagToKeys: make(map[string][]string),
		keyToTags: make(map[string][]string),
	}
}

// AddTags는 키에 태그를 추가합니다.
func (m *TagManager) AddTags(key string, tags ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, tag := range tags {
		m.tagToKeys[tag] = appendUnique(m.tagToKeys[tag], key)
		m.keyToTags[key] = appendUnique(m.keyToTags[key], tag)
	}
}

// RemoveTags는 키에서 태그를 제거합니다.
func (m *TagManager) RemoveTags(key string, tags ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, tag := range tags {
		m.tagToKeys[tag] = removeFromSlice(m.tagToKeys[tag], key)
		m.keyToTags[key] = removeFromSlice(m.keyToTags[key], tag)
	}
}

// GetKeysByTag는 태그에 해당하는 모든 키를 반환합니다.
func (m *TagManager) GetKeysByTag(tag string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := m.tagToKeys[tag]
	result := make([]string, len(keys))
	copy(result, keys)
	return result
}

// GetKeysByTags는 여러 태그 중 하나라도 가진 키를 반환합니다.
func (m *TagManager) GetKeysByTags(tags ...string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keySet := make(map[string]bool)
	for _, tag := range tags {
		for _, key := range m.tagToKeys[tag] {
			keySet[key] = true
		}
	}

	result := make([]string, 0, len(keySet))
	for key := range keySet {
		result = append(result, key)
	}
	return result
}

// GetTagsByKey는 키에 연결된 모든 태그를 반환합니다.
func (m *TagManager) GetTagsByKey(key string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tags := m.keyToTags[key]
	result := make([]string, len(tags))
	copy(result, tags)
	return result
}

// RemoveKey는 키와 관련된 모든 태그 매핑을 제거합니다.
func (m *TagManager) RemoveKey(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, tag := range m.keyToTags[key] {
		m.tagToKeys[tag] = removeFromSlice(m.tagToKeys[tag], key)
	}
	delete(m.keyToTags, key)
}

// ObjectPool은 pool.go에서 정의됨

// =============================================================================
// IntelligentCache: 지능형 캐시 (Cache 확장)
// =============================================================================

// IntelligentCache는 지능형 기능이 추가된 캐시입니다.
type IntelligentCache struct {
	*Cache

	// 지능형 옵션
	intelligentOpts *IntelligentOptions

	// 패턴 추적기
	patternTracker *PatternTracker

	// 적응형 TTL 관리자
	ttlManager *AdaptiveTTLManager

	// 의존성 그래프
	dependencyGraph *DependencyGraph

	// 태그 관리자
	tagManager *TagManager

	// 객체 풀
	entryPool     *EntryPool
	byteSlicePool *ByteSlicePool
}

// NewIntelligentCache는 새로운 지능형 캐시를 생성합니다.
func NewIntelligentCache(cacheOpts []CacheOption, intelligentOpts ...IntelligentOption) *IntelligentCache {
	// 기본 캐시 생성
	cache := New(cacheOpts...)

	// 지능형 옵션 처리
	intOpts := DefaultIntelligentOptions()
	for _, opt := range intelligentOpts {
		opt(intOpts)
	}

	ic := &IntelligentCache{
		Cache:           cache,
		intelligentOpts: intOpts,
		tagManager:      NewTagManager(),
	}

	// 예측 캐싱 활성화
	if intOpts.PredictiveCaching {
		ic.patternTracker = NewPatternTracker(1000)
	}

	// 적응형 TTL 활성화
	if intOpts.AdaptiveTTL {
		ic.ttlManager = NewAdaptiveTTLManager(intOpts)
	}

	// 의존성 추적 활성화
	if intOpts.DependencyTracking {
		ic.dependencyGraph = NewDependencyGraph(intOpts.MaxDependencyDepth)
	}

	// 객체 풀링 활성화
	if intOpts.ObjectPooling {
		ic.entryPool = NewEntryPool()
		ic.byteSlicePool = NewByteSlicePool(4096)
	}

	return ic
}

// Get은 지능형 기능이 추가된 조회입니다.
func (ic *IntelligentCache) Get(ctx context.Context, key string, dest interface{}) (int, error) {
	// 패턴 기록
	if ic.patternTracker != nil {
		relatedKeys := ic.patternTracker.RecordAccess(key)

		// 프리페치 (비동기)
		if len(relatedKeys) > 0 {
			go ic.prefetch(ctx, relatedKeys)
		}
	}

	return ic.Cache.Get(ctx, key, dest)
}

// Set은 지능형 기능이 추가된 저장입니다.
func (ic *IntelligentCache) Set(ctx context.Context, key string, value interface{}, opts ...SetOption) error {
	// 적응형 TTL 적용
	if ic.ttlManager != nil {
		ic.ttlManager.RecordUpdate(key)
		adaptiveTTL := ic.ttlManager.CalculateTTL(key)
		opts = append([]SetOption{WithTTL(adaptiveTTL)}, opts...)
	}

	return ic.Cache.Set(ctx, key, value, opts...)
}

// SetWithTags는 태그와 함께 저장합니다.
func (ic *IntelligentCache) SetWithTags(ctx context.Context, key string, value interface{}, tags []string, opts ...SetOption) error {
	if err := ic.Set(ctx, key, value, opts...); err != nil {
		return err
	}

	if ic.tagManager != nil && len(tags) > 0 {
		ic.tagManager.AddTags(key, tags...)
	}

	return nil
}

// SetWithDependencies는 의존성과 함께 저장합니다.
func (ic *IntelligentCache) SetWithDependencies(ctx context.Context, key string, value interface{}, dependsOn []string, opts ...SetOption) error {
	if err := ic.Set(ctx, key, value, opts...); err != nil {
		return err
	}

	if ic.dependencyGraph != nil && len(dependsOn) > 0 {
		ic.dependencyGraph.AddDependency(key, dependsOn...)
	}

	return nil
}

// InvalidateByTag는 태그에 해당하는 모든 키를 무효화합니다.
func (ic *IntelligentCache) InvalidateByTag(ctx context.Context, tag string) error {
	if ic.tagManager == nil {
		return nil
	}

	keys := ic.tagManager.GetKeysByTag(tag)
	for _, key := range keys {
		if err := ic.Delete(ctx, key); err != nil {
			return err
		}
	}

	return nil
}

// InvalidateByTags는 여러 태그에 해당하는 모든 키를 무효화합니다.
func (ic *IntelligentCache) InvalidateByTags(ctx context.Context, tags ...string) error {
	if ic.tagManager == nil {
		return nil
	}

	keys := ic.tagManager.GetKeysByTags(tags...)
	for _, key := range keys {
		if err := ic.Delete(ctx, key); err != nil {
			return err
		}
	}

	return nil
}

// InvalidateWithDependents는 키와 의존하는 모든 키를 무효화합니다.
func (ic *IntelligentCache) InvalidateWithDependents(ctx context.Context, key string) error {
	// 의존하는 키들 찾기
	var keysToDelete []string
	if ic.dependencyGraph != nil {
		keysToDelete = ic.dependencyGraph.GetDependents(key)
	}
	keysToDelete = append([]string{key}, keysToDelete...)

	// 모두 삭제
	for _, k := range keysToDelete {
		if err := ic.Delete(ctx, k); err != nil {
			return err
		}

		// 태그 매핑 제거
		if ic.tagManager != nil {
			ic.tagManager.RemoveKey(k)
		}

		// 의존성 그래프에서 제거
		if ic.dependencyGraph != nil {
			ic.dependencyGraph.Clear(k)
		}
	}

	return nil
}

// prefetch는 연관 키들을 미리 로드합니다.
func (ic *IntelligentCache) prefetch(ctx context.Context, keys []string) {
	// L2 이하에서 L1으로 미리 로드
	for _, key := range keys {
		var temp interface{}
		ic.Cache.Get(ctx, key, &temp)
	}
}

// =============================================================================
// 헬퍼 함수
// =============================================================================

// appendUnique는 중복 없이 슬라이스에 추가합니다.
func appendUnique(slice []string, item string) []string {
	for _, s := range slice {
		if s == item {
			return slice
		}
	}
	return append(slice, item)
}

// removeFromSlice는 슬라이스에서 항목을 제거합니다.
func removeFromSlice(slice []string, item string) []string {
	for i, s := range slice {
		if s == item {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// min은 두 float64 중 작은 값을 반환합니다.
func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// max는 두 float64 중 큰 값을 반환합니다.
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
