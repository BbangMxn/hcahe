// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 캐시 설정 옵션을 정의합니다.
package core

import (
	"time"
)

// =============================================================================
// WriteMode: 저장 모드
// =============================================================================
// WriteMode는 데이터 저장 시 동기화 수준을 결정합니다.
// 속도와 안정성 사이의 트레이드오프를 조절할 수 있습니다.
// =============================================================================

// WriteMode는 캐시 쓰기 모드를 나타냅니다.
type WriteMode int

const (
	// Hot은 L1에만 동기 저장하고, 나머지는 비동기로 전파합니다.
	// 가장 빠르지만 L1 장애 시 데이터 손실 가능성이 있습니다.
	// 기본값입니다.
	Hot WriteMode = iota

	// Warm은 L1, L2까지 동기 저장하고, L3 이하는 비동기로 전파합니다.
	// 속도와 안정성의 균형을 제공합니다.
	Warm

	// Cold는 모든 계층에 동기 저장합니다.
	// 가장 느리지만 데이터 손실 가능성이 없습니다.
	Cold
)

// String은 WriteMode의 문자열 표현을 반환합니다.
func (m WriteMode) String() string {
	switch m {
	case Hot:
		return "hot"
	case Warm:
		return "warm"
	case Cold:
		return "cold"
	default:
		return "unknown"
	}
}

// =============================================================================
// EvictionPolicy: 퇴거 정책
// =============================================================================
// 캐시가 가득 찼을 때 어떤 항목을 제거할지 결정합니다.
// =============================================================================

// EvictionPolicy는 캐시 퇴거 정책을 나타냅니다.
type EvictionPolicy int

const (
	// LRU는 가장 오래 사용되지 않은 항목을 제거합니다.
	// 대부분의 경우 좋은 성능을 보입니다.
	LRU EvictionPolicy = iota

	// LFU는 가장 적게 사용된 항목을 제거합니다.
	// 인기 있는 항목을 오래 유지합니다.
	LFU

	// FIFO는 가장 먼저 들어온 항목을 제거합니다.
	// 구현이 단순하고 예측 가능합니다.
	FIFO

	// Random은 무작위로 항목을 제거합니다.
	// 오버헤드가 가장 적습니다.
	Random

	// ARC는 적응형 교체 캐시로, LRU와 LFU의 장점을 결합합니다.
	// 더 복잡하지만 다양한 워크로드에서 좋은 성능을 보입니다.
	ARC
)

// String은 EvictionPolicy의 문자열 표현을 반환합니다.
func (p EvictionPolicy) String() string {
	switch p {
	case LRU:
		return "lru"
	case LFU:
		return "lfu"
	case FIFO:
		return "fifo"
	case Random:
		return "random"
	case ARC:
		return "arc"
	default:
		return "unknown"
	}
}

// =============================================================================
// CacheOptions: 전역 캐시 옵션
// =============================================================================

// CacheOptions는 캐시의 전역 설정을 정의합니다.
type CacheOptions struct {
	// DefaultTTL은 TTL이 지정되지 않은 항목의 기본 만료 시간입니다.
	// 0이면 만료되지 않습니다.
	DefaultTTL time.Duration

	// DefaultWriteMode는 기본 쓰기 모드입니다.
	DefaultWriteMode WriteMode

	// Serializer는 사용할 직렬화 방식입니다.
	// "json", "msgpack", "protobuf" 중 하나입니다.
	Serializer string

	// CompressionEnabled는 압축 사용 여부입니다.
	CompressionEnabled bool

	// CompressionThreshold는 압축을 적용할 최소 크기(바이트)입니다.
	// 이 크기보다 작은 데이터는 압축하지 않습니다.
	CompressionThreshold int

	// CompressionType은 사용할 압축 알고리즘입니다.
	// "gzip", "lz4", "zstd" 중 하나입니다.
	CompressionType string

	// EnableMetrics는 메트릭 수집 활성화 여부입니다.
	EnableMetrics bool

	// EnableAutoTiering은 자동 티어링 활성화 여부입니다.
	EnableAutoTiering bool

	// PromotionThreshold는 승격을 위한 최소 접근 횟수입니다.
	// 이 횟수 이상 접근된 항목은 상위 계층으로 승격됩니다.
	PromotionThreshold uint64

	// DemotionIdleTime은 강등을 위한 유휴 시간입니다.
	// 이 시간 이상 접근되지 않은 항목은 하위 계층으로 강등됩니다.
	DemotionIdleTime time.Duration

	// WriteBufferSize는 비동기 쓰기 버퍼의 크기입니다.
	WriteBufferSize int

	// WriteBufferFlushInterval은 버퍼 플러시 주기입니다.
	WriteBufferFlushInterval time.Duration

	// MaxRetries는 실패 시 최대 재시도 횟수입니다.
	MaxRetries int

	// RetryDelay는 재시도 간 대기 시간입니다.
	RetryDelay time.Duration
}

// DefaultCacheOptions는 기본 캐시 옵션을 반환합니다.
func DefaultCacheOptions() *CacheOptions {
	return &CacheOptions{
		DefaultTTL:               0, // 만료 없음
		DefaultWriteMode:         Hot,
		Serializer:               "msgpack",
		CompressionEnabled:       true,
		CompressionThreshold:     1024, // 1KB
		CompressionType:          "lz4",
		EnableMetrics:            true,
		EnableAutoTiering:        true,
		PromotionThreshold:       5,
		DemotionIdleTime:         5 * time.Minute,
		WriteBufferSize:          1000,
		WriteBufferFlushInterval: 100 * time.Millisecond,
		MaxRetries:               3,
		RetryDelay:               10 * time.Millisecond,
	}
}

// =============================================================================
// LayerOptions: 계층별 옵션
// =============================================================================

// LayerOptions는 개별 캐시 계층의 설정을 정의합니다.
type LayerOptions struct {
	// Name은 계층의 이름입니다. (예: "memory", "redis", "sqlite")
	Name string

	// Priority는 계층의 우선순위입니다.
	// 숫자가 낮을수록 먼저 조회됩니다. (L1 = 0, L2 = 1, ...)
	Priority int

	// MaxSize는 이 계층이 저장할 수 있는 최대 항목 수입니다.
	// 0이면 제한 없음입니다.
	MaxSize int

	// MaxMemory는 이 계층이 사용할 수 있는 최대 메모리(바이트)입니다.
	// 0이면 제한 없음입니다.
	MaxMemory int64

	// TTL은 이 계층의 기본 TTL입니다.
	// 전역 DefaultTTL보다 우선합니다.
	TTL time.Duration

	// TTLMultiplier는 전역 TTL에 곱할 배수입니다.
	// 예: 0.2면 전역 TTL의 20%, 3.0이면 300%
	// TTL이 설정되어 있으면 무시됩니다.
	TTLMultiplier float64

	// EvictionPolicy는 이 계층의 퇴거 정책입니다.
	EvictionPolicy EvictionPolicy

	// SyncWrite는 이 계층에 동기 쓰기를 강제할지 여부입니다.
	// true면 WriteMode와 관계없이 항상 동기 쓰기합니다.
	SyncWrite bool

	// ReadOnly는 이 계층을 읽기 전용으로 설정합니다.
	// 주로 공유 캐시를 읽기만 할 때 사용합니다.
	ReadOnly bool

	// SkipOnMiss는 캐시 미스 시 이 계층을 건너뛸지 여부입니다.
	// true면 상위 계층 미스 시 이 계층을 조회하지 않습니다.
	SkipOnMiss bool
}

// DefaultLayerOptions는 기본 계층 옵션을 반환합니다.
func DefaultLayerOptions(name string, priority int) *LayerOptions {
	return &LayerOptions{
		Name:           name,
		Priority:       priority,
		MaxSize:        0, // 제한 없음
		MaxMemory:      0, // 제한 없음
		TTL:            0, // 전역 설정 따름
		TTLMultiplier:  1.0,
		EvictionPolicy: LRU,
		SyncWrite:      false,
		ReadOnly:       false,
		SkipOnMiss:     false,
	}
}

// =============================================================================
// SetOptions: 개별 Set 호출 옵션
// =============================================================================

// SetOptions는 개별 Set 호출의 옵션을 정의합니다.
type SetOptions struct {
	// TTL은 이 항목의 만료 시간입니다.
	// 0이면 기본값 사용, -1이면 만료 없음입니다.
	TTL time.Duration

	// Mode는 이 쓰기의 모드입니다.
	// nil이면 기본 모드를 사용합니다.
	Mode *WriteMode

	// TargetLayers는 저장할 계층을 지정합니다.
	// nil이면 모든 계층에 저장합니다.
	TargetLayers []int

	// SkipCompression은 이 항목의 압축을 건너뛸지 여부입니다.
	SkipCompression bool

	// Tags는 이 항목에 연결할 태그입니다.
	// 태그 기반 무효화에 사용됩니다.
	Tags []string
}

// DefaultSetOptions는 기본 Set 옵션을 반환합니다.
func DefaultSetOptions() *SetOptions {
	return &SetOptions{
		TTL:             0,
		Mode:            nil,
		TargetLayers:    nil,
		SkipCompression: false,
		Tags:            nil,
	}
}

// =============================================================================
// Functional Options Pattern
// =============================================================================
// 함수형 옵션 패턴으로 유연한 설정을 지원합니다.
// =============================================================================

// CacheOption은 캐시 옵션을 설정하는 함수입니다.
type CacheOption func(*CacheOptions)

// WithDefaultTTL은 기본 TTL을 설정합니다.
func WithDefaultTTL(ttl time.Duration) CacheOption {
	return func(o *CacheOptions) {
		o.DefaultTTL = ttl
	}
}

// WithDefaultWriteMode는 기본 쓰기 모드를 설정합니다.
func WithDefaultWriteMode(mode WriteMode) CacheOption {
	return func(o *CacheOptions) {
		o.DefaultWriteMode = mode
	}
}

// WithSerializer는 직렬화 방식을 설정합니다.
func WithSerializer(serializer string) CacheOption {
	return func(o *CacheOptions) {
		o.Serializer = serializer
	}
}

// WithCompression은 압축을 활성화하고 설정합니다.
func WithCompression(compressionType string, threshold int) CacheOption {
	return func(o *CacheOptions) {
		o.CompressionEnabled = true
		o.CompressionType = compressionType
		o.CompressionThreshold = threshold
	}
}

// WithoutCompression은 압축을 비활성화합니다.
func WithoutCompression() CacheOption {
	return func(o *CacheOptions) {
		o.CompressionEnabled = false
	}
}

// WithMetrics는 메트릭 수집을 활성화합니다.
func WithMetrics() CacheOption {
	return func(o *CacheOptions) {
		o.EnableMetrics = true
	}
}

// WithAutoTiering은 자동 티어링을 설정합니다.
func WithAutoTiering(promotionThreshold uint64, demotionIdleTime time.Duration) CacheOption {
	return func(o *CacheOptions) {
		o.EnableAutoTiering = true
		o.PromotionThreshold = promotionThreshold
		o.DemotionIdleTime = demotionIdleTime
	}
}

// WithoutAutoTiering은 자동 티어링을 비활성화합니다.
func WithoutAutoTiering() CacheOption {
	return func(o *CacheOptions) {
		o.EnableAutoTiering = false
	}
}

// WithWriteBuffer는 쓰기 버퍼를 설정합니다.
func WithWriteBuffer(size int, flushInterval time.Duration) CacheOption {
	return func(o *CacheOptions) {
		o.WriteBufferSize = size
		o.WriteBufferFlushInterval = flushInterval
	}
}

// WithRetry는 재시도 설정을 지정합니다.
func WithRetry(maxRetries int, delay time.Duration) CacheOption {
	return func(o *CacheOptions) {
		o.MaxRetries = maxRetries
		o.RetryDelay = delay
	}
}

// =============================================================================
// SetOption: Set 호출 옵션 함수
// =============================================================================

// SetOption은 Set 호출 옵션을 설정하는 함수입니다.
type SetOption func(*SetOptions)

// WithTTL은 TTL을 설정합니다.
func WithTTL(ttl time.Duration) SetOption {
	return func(o *SetOptions) {
		o.TTL = ttl
	}
}

// WithMode는 쓰기 모드를 설정합니다.
func WithMode(mode WriteMode) SetOption {
	return func(o *SetOptions) {
		o.Mode = &mode
	}
}

// WithTargetLayers는 저장할 계층을 지정합니다.
func WithTargetLayers(layers ...int) SetOption {
	return func(o *SetOptions) {
		o.TargetLayers = layers
	}
}

// WithSkipCompression은 압축을 건너뜁니다.
func WithSkipCompression() SetOption {
	return func(o *SetOptions) {
		o.SkipCompression = true
	}
}

// WithTags는 태그를 설정합니다.
func WithTags(tags ...string) SetOption {
	return func(o *SetOptions) {
		o.Tags = tags
	}
}
