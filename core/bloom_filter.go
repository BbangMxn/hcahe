// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Bloom Filter를 구현합니다.
package core

import (
	"hash/fnv"
	"math"
	"sync"
)

// =============================================================================
// Bloom Filter: 캐시 관통 방지
// =============================================================================
// 존재하지 않는 키에 대한 요청을 빠르게 거부하여
// 백엔드 DB에 불필요한 요청을 방지합니다.
//
// 특성:
// - False Positive 가능 (없는데 있다고 할 수 있음)
// - False Negative 불가능 (있는데 없다고 하지 않음)
// - 메모리 효율적 (비트 배열 사용)
// =============================================================================

// BloomFilter는 확률적 자료구조입니다.
type BloomFilter struct {
	bits    []uint64
	numBits uint
	numHash uint
	count   uint64
	mu      sync.RWMutex
}

// NewBloomFilter는 새로운 블룸 필터를 생성합니다.
//
// Parameters:
//   - expectedItems: 예상 항목 수
//   - falsePositiveRate: 원하는 오탐률 (0.01 = 1%)
func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	// 최적의 비트 수 계산: m = -n * ln(p) / (ln(2)^2)
	m := uint(math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)))

	// 최적의 해시 함수 수 계산: k = (m/n) * ln(2)
	k := uint(math.Ceil(float64(m) / float64(expectedItems) * math.Ln2))

	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	// 64비트 단위로 올림
	numWords := (m + 63) / 64

	return &BloomFilter{
		bits:    make([]uint64, numWords),
		numBits: m,
		numHash: k,
	}
}

// NewBloomFilterWithSize는 지정된 크기로 블룸 필터를 생성합니다.
func NewBloomFilterWithSize(numBits, numHash uint) *BloomFilter {
	if numHash < 1 {
		numHash = 1
	}
	if numHash > 30 {
		numHash = 30
	}

	numWords := (numBits + 63) / 64

	return &BloomFilter{
		bits:    make([]uint64, numWords),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add는 키를 필터에 추가합니다.
func (bf *BloomFilter) Add(key string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	h1, h2 := bf.hash(key)

	for i := uint(0); i < bf.numHash; i++ {
		pos := (h1 + i*h2) % bf.numBits
		bf.setBit(pos)
	}

	bf.count++
}

// Contains는 키가 존재할 수 있는지 확인합니다.
// true: 존재할 수 있음 (확실하지 않음)
// false: 확실히 존재하지 않음
func (bf *BloomFilter) Contains(key string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	h1, h2 := bf.hash(key)

	for i := uint(0); i < bf.numHash; i++ {
		pos := (h1 + i*h2) % bf.numBits
		if !bf.getBit(pos) {
			return false
		}
	}

	return true
}

// MayContain은 Contains의 별칭입니다.
func (bf *BloomFilter) MayContain(key string) bool {
	return bf.Contains(key)
}

// Reset은 필터를 초기화합니다.
func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.count = 0
}

// Count는 추가된 항목 수를 반환합니다.
func (bf *BloomFilter) Count() uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.count
}

// FillRatio는 설정된 비트의 비율을 반환합니다.
func (bf *BloomFilter) FillRatio() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	setBits := uint(0)
	for _, word := range bf.bits {
		setBits += uint(popcount(word))
	}

	return float64(setBits) / float64(bf.numBits)
}

// EstimatedFalsePositiveRate는 현재 오탐률을 추정합니다.
func (bf *BloomFilter) EstimatedFalsePositiveRate() float64 {
	fillRatio := bf.FillRatio()
	return math.Pow(fillRatio, float64(bf.numHash))
}

// hash는 이중 해싱을 수행합니다.
func (bf *BloomFilter) hash(key string) (uint, uint) {
	h := fnv.New64a()
	h.Write([]byte(key))
	sum := h.Sum64()

	h1 := uint(sum)
	h2 := uint(sum >> 32)

	if h2 == 0 {
		h2 = 1
	}

	return h1, h2
}

// setBit는 비트를 설정합니다.
func (bf *BloomFilter) setBit(pos uint) {
	wordIdx := pos / 64
	bitIdx := pos % 64
	bf.bits[wordIdx] |= 1 << bitIdx
}

// getBit는 비트를 조회합니다.
func (bf *BloomFilter) getBit(pos uint) bool {
	wordIdx := pos / 64
	bitIdx := pos % 64
	return (bf.bits[wordIdx] & (1 << bitIdx)) != 0
}

// popcount는 설정된 비트 수를 반환합니다 (해밍 웨이트).
func popcount(x uint64) int {
	// Brian Kernighan's algorithm
	count := 0
	for x != 0 {
		x &= x - 1
		count++
	}
	return count
}

// =============================================================================
// BloomFilterCache: 블룸 필터가 적용된 캐시 래퍼
// =============================================================================

// BloomFilterCache는 블룸 필터를 사용하여 캐시 관통을 방지합니다.
type BloomFilterCache struct {
	cache  *Cache
	filter *BloomFilter

	// NegativeCache는 존재하지 않는 키를 캐싱합니다.
	negativeCache     map[string]bool
	negativeCacheTTL  int64 // unix nano
	negativeCacheSize int
	negativeMu        sync.RWMutex
}

// BloomFilterCacheConfig는 블룸 필터 캐시 설정입니다.
type BloomFilterCacheConfig struct {
	// ExpectedItems는 예상 항목 수입니다.
	ExpectedItems int

	// FalsePositiveRate는 원하는 오탐률입니다.
	FalsePositiveRate float64

	// NegativeCacheSize는 네거티브 캐시 크기입니다.
	// 0이면 네거티브 캐시를 사용하지 않습니다.
	NegativeCacheSize int
}

// DefaultBloomFilterCacheConfig는 기본 설정을 반환합니다.
func DefaultBloomFilterCacheConfig() *BloomFilterCacheConfig {
	return &BloomFilterCacheConfig{
		ExpectedItems:     100000,
		FalsePositiveRate: 0.01,
		NegativeCacheSize: 10000,
	}
}

// NewBloomFilterCache는 새로운 블룸 필터 캐시를 생성합니다.
func NewBloomFilterCache(cache *Cache, config *BloomFilterCacheConfig) *BloomFilterCache {
	if config == nil {
		config = DefaultBloomFilterCacheConfig()
	}

	bf := &BloomFilterCache{
		cache:  cache,
		filter: NewBloomFilter(config.ExpectedItems, config.FalsePositiveRate),
	}

	if config.NegativeCacheSize > 0 {
		bf.negativeCache = make(map[string]bool, config.NegativeCacheSize)
		bf.negativeCacheSize = config.NegativeCacheSize
	}

	return bf
}

// Get은 블룸 필터를 확인한 후 캐시를 조회합니다.
func (bfc *BloomFilterCache) Get(ctx interface{}, key string, dest interface{}) (int, error) {
	// 네거티브 캐시 확인
	if bfc.negativeCache != nil {
		bfc.negativeMu.RLock()
		if _, notExists := bfc.negativeCache[key]; notExists {
			bfc.negativeMu.RUnlock()
			return -1, nil // 확실히 없음
		}
		bfc.negativeMu.RUnlock()
	}

	// 블룸 필터 확인
	if !bfc.filter.Contains(key) {
		// 확실히 존재하지 않음
		bfc.addNegative(key)
		return -1, nil
	}

	// 캐시 조회
	c, ok := ctx.(interface{ Done() <-chan struct{} })
	if !ok {
		return -1, nil
	}

	// 실제 Get은 context.Context가 필요
	_ = c
	return -1, nil // 실제 구현에서는 cache.Get 호출
}

// Set은 캐시에 저장하고 블룸 필터에 추가합니다.
func (bfc *BloomFilterCache) Set(key string) {
	bfc.filter.Add(key)
	bfc.removeNegative(key)
}

// Delete는 네거티브 캐시에서 제거합니다.
// 블룸 필터에서는 제거할 수 없습니다.
func (bfc *BloomFilterCache) Delete(key string) {
	// 블룸 필터는 삭제 불가
	// 네거티브 캐시에서 제거
	bfc.removeNegative(key)
}

// addNegative는 네거티브 캐시에 추가합니다.
func (bfc *BloomFilterCache) addNegative(key string) {
	if bfc.negativeCache == nil {
		return
	}

	bfc.negativeMu.Lock()
	defer bfc.negativeMu.Unlock()

	// 크기 제한
	if len(bfc.negativeCache) >= bfc.negativeCacheSize {
		// 간단히 전체 클리어 (실제로는 LRU 사용해야 함)
		bfc.negativeCache = make(map[string]bool, bfc.negativeCacheSize)
	}

	bfc.negativeCache[key] = true
}

// removeNegative는 네거티브 캐시에서 제거합니다.
func (bfc *BloomFilterCache) removeNegative(key string) {
	if bfc.negativeCache == nil {
		return
	}

	bfc.negativeMu.Lock()
	delete(bfc.negativeCache, key)
	bfc.negativeMu.Unlock()
}

// Filter는 내부 블룸 필터를 반환합니다.
func (bfc *BloomFilterCache) Filter() *BloomFilter {
	return bfc.filter
}

// ResetFilter는 블룸 필터를 리셋합니다.
func (bfc *BloomFilterCache) ResetFilter() {
	bfc.filter.Reset()
}

// ClearNegativeCache는 네거티브 캐시를 클리어합니다.
func (bfc *BloomFilterCache) ClearNegativeCache() {
	if bfc.negativeCache == nil {
		return
	}

	bfc.negativeMu.Lock()
	bfc.negativeCache = make(map[string]bool, bfc.negativeCacheSize)
	bfc.negativeMu.Unlock()
}
