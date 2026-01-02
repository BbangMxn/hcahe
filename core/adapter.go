// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 저장소 어댑터 인터페이스를 정의합니다.
package core

import (
	"context"
	"io"
)

// =============================================================================
// Adapter Interface: 모든 저장소 백엔드의 공통 인터페이스
// =============================================================================
// Adapter는 다양한 저장소(Memory, Redis, SQLite, S3 등)를 추상화합니다.
// 새로운 저장소를 추가하려면 이 인터페이스를 구현하면 됩니다.
// =============================================================================

// Adapter는 캐시 저장소 백엔드의 인터페이스입니다.
type Adapter interface {
	// ==========================================================================
	// 기본 정보
	// ==========================================================================

	// Name은 어댑터의 이름을 반환합니다.
	// 예: "memory", "redis", "sqlite", "s3"
	Name() string

	// Type은 어댑터의 유형을 반환합니다.
	Type() AdapterType

	// ==========================================================================
	// 연결 관리
	// ==========================================================================

	// Connect는 저장소에 연결합니다.
	// 이미 연결된 경우 아무 작업도 하지 않습니다.
	Connect(ctx context.Context) error

	// Disconnect는 저장소 연결을 종료합니다.
	// 연결되지 않은 경우 아무 작업도 하지 않습니다.
	Disconnect(ctx context.Context) error

	// IsConnected는 연결 상태를 반환합니다.
	IsConnected() bool

	// Ping은 저장소가 응답하는지 확인합니다.
	Ping(ctx context.Context) error

	// ==========================================================================
	// CRUD 연산
	// ==========================================================================

	// Get은 키에 해당하는 엔트리를 조회합니다.
	// 키가 없으면 (nil, nil)을 반환합니다.
	// 에러 발생 시 (nil, error)를 반환합니다.
	Get(ctx context.Context, key string) (*Entry, error)

	// Set은 엔트리를 저장합니다.
	// 이미 존재하는 키면 덮어씁니다.
	Set(ctx context.Context, entry *Entry) error

	// Delete는 키에 해당하는 엔트리를 삭제합니다.
	// 키가 없어도 에러를 반환하지 않습니다.
	// 반환값은 실제로 삭제되었는지 여부입니다.
	Delete(ctx context.Context, key string) (bool, error)

	// Has는 키가 존재하는지 확인합니다.
	Has(ctx context.Context, key string) (bool, error)

	// ==========================================================================
	// 배치 연산 (성능 최적화)
	// ==========================================================================

	// GetMany는 여러 키를 한 번에 조회합니다.
	// 없는 키는 결과 맵에 포함되지 않습니다.
	GetMany(ctx context.Context, keys []string) (map[string]*Entry, error)

	// SetMany는 여러 엔트리를 한 번에 저장합니다.
	SetMany(ctx context.Context, entries []*Entry) error

	// DeleteMany는 여러 키를 한 번에 삭제합니다.
	// 반환값은 실제로 삭제된 개수입니다.
	DeleteMany(ctx context.Context, keys []string) (int, error)

	// ==========================================================================
	// 관리
	// ==========================================================================

	// Keys는 패턴에 매칭되는 모든 키를 반환합니다.
	// 패턴은 glob 스타일입니다. (예: "user:*", "*:session")
	// 빈 패턴("")은 모든 키를 반환합니다.
	Keys(ctx context.Context, pattern string) ([]string, error)

	// Clear는 모든 엔트리를 삭제합니다.
	Clear(ctx context.Context) error

	// Size는 저장된 엔트리 개수를 반환합니다.
	Size(ctx context.Context) (int64, error)

	// ==========================================================================
	// 메트릭
	// ==========================================================================

	// Stats는 어댑터의 통계를 반환합니다.
	Stats(ctx context.Context) (*AdapterStats, error)
}

// =============================================================================
// AdapterType: 어댑터 유형
// =============================================================================

// AdapterType은 어댑터의 유형을 나타냅니다.
type AdapterType int

const (
	// TypeMemory는 인프로세스 메모리 저장소입니다.
	// 가장 빠르지만 프로세스 종료 시 데이터가 사라집니다.
	TypeMemory AdapterType = iota

	// TypeNetwork는 네트워크 기반 저장소입니다.
	// Redis, Memcached 등이 해당됩니다.
	TypeNetwork

	// TypeDisk는 로컬 디스크 저장소입니다.
	// SQLite, RocksDB, LevelDB 등이 해당됩니다.
	TypeDisk

	// TypeRemote는 원격 저장소입니다.
	// S3, GCS, Azure Blob 등이 해당됩니다.
	TypeRemote

	// TypeDatabase는 범용 데이터베이스입니다.
	// PostgreSQL, MySQL, MongoDB 등이 해당됩니다.
	TypeDatabase
)

// String은 AdapterType의 문자열 표현을 반환합니다.
func (t AdapterType) String() string {
	switch t {
	case TypeMemory:
		return "memory"
	case TypeNetwork:
		return "network"
	case TypeDisk:
		return "disk"
	case TypeRemote:
		return "remote"
	case TypeDatabase:
		return "database"
	default:
		return "unknown"
	}
}

// ExpectedLatency는 예상 지연 시간을 반환합니다 (나노초).
func (t AdapterType) ExpectedLatency() int64 {
	switch t {
	case TypeMemory:
		return 1000 // 1μs
	case TypeNetwork:
		return 1000000 // 1ms
	case TypeDisk:
		return 5000000 // 5ms
	case TypeRemote:
		return 100000000 // 100ms
	case TypeDatabase:
		return 10000000 // 10ms
	default:
		return 100000000
	}
}

// =============================================================================
// AdapterStats: 어댑터 통계
// =============================================================================

// AdapterStats는 어댑터의 통계 정보를 담습니다.
type AdapterStats struct {
	// Name은 어댑터 이름입니다.
	Name string `json:"name"`

	// Type은 어댑터 유형입니다.
	Type AdapterType `json:"type"`

	// Connected는 연결 상태입니다.
	Connected bool `json:"connected"`

	// Size는 저장된 엔트리 개수입니다.
	Size int64 `json:"size"`

	// MaxSize는 최대 저장 가능 개수입니다 (0이면 무제한).
	MaxSize int64 `json:"max_size"`

	// MemoryUsage는 사용 중인 메모리(바이트)입니다.
	MemoryUsage int64 `json:"memory_usage"`

	// MaxMemory는 최대 사용 가능 메모리입니다 (0이면 무제한).
	MaxMemory int64 `json:"max_memory"`

	// Hits는 캐시 히트 횟수입니다.
	Hits uint64 `json:"hits"`

	// Misses는 캐시 미스 횟수입니다.
	Misses uint64 `json:"misses"`

	// GetLatencyNs는 평균 Get 지연 시간(나노초)입니다.
	GetLatencyNs int64 `json:"get_latency_ns"`

	// SetLatencyNs는 평균 Set 지연 시간(나노초)입니다.
	SetLatencyNs int64 `json:"set_latency_ns"`

	// Evictions는 퇴거된 엔트리 개수입니다.
	Evictions uint64 `json:"evictions"`

	// Expirations는 만료된 엔트리 개수입니다.
	Expirations uint64 `json:"expirations"`

	// CircuitState는 Circuit Breaker 상태입니다.
	// "closed" (정상), "open" (차단), "half-open" (테스트 중)
	CircuitState string `json:"circuit_state,omitempty"`
}

// HitRate는 캐시 히트율을 반환합니다 (0.0 ~ 1.0).
func (s *AdapterStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// UsageRate는 용량 사용률을 반환합니다 (0.0 ~ 1.0).
func (s *AdapterStats) UsageRate() float64 {
	if s.MaxSize == 0 {
		return 0
	}
	return float64(s.Size) / float64(s.MaxSize)
}

// MemoryUsageRate는 메모리 사용률을 반환합니다 (0.0 ~ 1.0).
func (s *AdapterStats) MemoryUsageRate() float64 {
	if s.MaxMemory == 0 {
		return 0
	}
	return float64(s.MemoryUsage) / float64(s.MaxMemory)
}

// =============================================================================
// StreamAdapter: 스트리밍 지원 어댑터 (선택적)
// =============================================================================
// 큰 데이터를 스트리밍으로 처리할 수 있는 어댑터용입니다.
// S3, GCS 등 원격 저장소에서 주로 사용됩니다.
// =============================================================================

// StreamAdapter는 스트리밍을 지원하는 어댑터입니다.
type StreamAdapter interface {
	Adapter

	// GetStream은 데이터를 스트리밍으로 읽습니다.
	GetStream(ctx context.Context, key string) (io.ReadCloser, error)

	// SetStream은 데이터를 스트리밍으로 씁니다.
	SetStream(ctx context.Context, key string, reader io.Reader, size int64) error
}

// =============================================================================
// BatchAdapter: 고급 배치 연산 (선택적)
// =============================================================================

// BatchAdapter는 고급 배치 연산을 지원하는 어댑터입니다.
type BatchAdapter interface {
	Adapter

	// Pipeline은 여러 연산을 파이프라인으로 실행합니다.
	// Redis의 파이프라인처럼 네트워크 라운드트립을 줄입니다.
	Pipeline(ctx context.Context, ops []BatchOp) ([]BatchResult, error)
}

// BatchOp는 배치 연산을 나타냅니다.
type BatchOp struct {
	Op    string // "get", "set", "delete"
	Key   string
	Entry *Entry // Set일 때만 사용
}

// BatchResult는 배치 연산 결과를 나타냅니다.
type BatchResult struct {
	Entry *Entry
	Error error
}

// =============================================================================
// TTLAdapter: TTL 네이티브 지원 (선택적)
// =============================================================================

// TTLAdapter는 TTL을 네이티브로 지원하는 어댑터입니다.
// Redis처럼 저장소 자체에서 TTL을 관리하는 경우 구현합니다.
type TTLAdapter interface {
	Adapter

	// SetWithTTL은 TTL과 함께 엔트리를 저장합니다.
	// 저장소의 네이티브 TTL 기능을 사용합니다.
	SetWithTTL(ctx context.Context, entry *Entry) error

	// GetTTL은 키의 남은 TTL을 반환합니다.
	GetTTL(ctx context.Context, key string) (int64, error)

	// SetTTL은 기존 키의 TTL을 변경합니다.
	SetTTL(ctx context.Context, key string, ttl int64) error
}

// =============================================================================
// AdapterFactory: 어댑터 팩토리
// =============================================================================

// AdapterConfig는 어댑터 생성 설정입니다.
type AdapterConfig struct {
	Type    string                 `json:"type"`
	Options map[string]interface{} `json:"options"`
}

// AdapterFactory는 설정에서 어댑터를 생성하는 팩토리입니다.
type AdapterFactory interface {
	// Create는 설정에 따라 어댑터를 생성합니다.
	Create(config AdapterConfig) (Adapter, error)

	// Register는 새로운 어댑터 타입을 등록합니다.
	Register(name string, creator func(map[string]interface{}) (Adapter, error))
}
