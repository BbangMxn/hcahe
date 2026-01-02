// Package redis는 Redis 캐시 어댑터를 구현합니다.
// L2 캐시로 사용되며, 여러 프로세스 간 캐시 공유가 가능합니다.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bridgify/hcache/core"
	"github.com/redis/go-redis/v9"
)

// =============================================================================
// RedisAdapter: Redis 캐시 어댑터
// =============================================================================
// RedisAdapter는 Redis에 데이터를 저장합니다.
// 네트워크 기반이지만 매우 빠르며, 여러 인스턴스 간 캐시 공유가 가능합니다.
//
// 특징:
// - ~1ms 접근 시간
// - 프로세스 간 공유 가능
// - 네이티브 TTL 지원
// - Pub/Sub으로 캐시 무효화 전파 가능
// =============================================================================

// Config는 Redis 어댑터 설정입니다.
type Config struct {
	// Addr은 Redis 서버 주소입니다. (예: "localhost:6379")
	Addr string

	// Password는 Redis 인증 비밀번호입니다.
	Password string

	// DB는 Redis 데이터베이스 번호입니다. (기본: 0)
	DB int

	// KeyPrefix는 모든 키에 붙는 접두사입니다.
	// 같은 Redis를 여러 용도로 사용할 때 유용합니다.
	KeyPrefix string

	// PoolSize는 연결 풀 크기입니다.
	PoolSize int

	// MinIdleConns는 최소 유휴 연결 수입니다.
	MinIdleConns int

	// DialTimeout은 연결 타임아웃입니다.
	DialTimeout time.Duration

	// ReadTimeout은 읽기 타임아웃입니다.
	ReadTimeout time.Duration

	// WriteTimeout은 쓰기 타임아웃입니다.
	WriteTimeout time.Duration

	// MaxRetries는 최대 재시도 횟수입니다.
	MaxRetries int
}

// DefaultConfig는 기본 설정을 반환합니다.
func DefaultConfig() *Config {
	return &Config{
		Addr:         "", // 환경변수 REDIS_ADDR 사용 권장
		Password:     "",
		DB:           0,
		KeyPrefix:    "hcache:",
		PoolSize:     100,
		MinIdleConns: 10,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		MaxRetries:   3,
	}
}

// Adapter는 Redis 캐시 어댑터입니다.
type Adapter struct {
	// config는 어댑터 설정입니다.
	config *Config

	// client는 Redis 클라이언트입니다.
	client *redis.Client

	// connected는 연결 상태입니다.
	connected bool

	// 메트릭
	hits       uint64
	misses     uint64
	getLatency int64
	getCount   uint64
	setLatency int64
	setCount   uint64

	// mu는 연결 상태 변경을 위한 뮤텍스입니다.
	mu sync.RWMutex
}

// =============================================================================
// Adapter 생성자
// =============================================================================

// New는 새로운 Redis 어댑터를 생성합니다.
//
// Parameters:
//   - config: 어댑터 설정 (nil이면 기본값)
//
// Returns:
//   - *Adapter: 생성된 어댑터
func New(config *Config) *Adapter {
	if config == nil {
		config = DefaultConfig()
	}

	return &Adapter{
		config: config,
	}
}

// =============================================================================
// Adapter 인터페이스 구현 - 기본 정보
// =============================================================================

// Name은 어댑터 이름을 반환합니다.
func (a *Adapter) Name() string {
	return "redis"
}

// Type은 어댑터 유형을 반환합니다.
func (a *Adapter) Type() core.AdapterType {
	return core.TypeNetwork
}

// =============================================================================
// Adapter 인터페이스 구현 - 연결 관리
// =============================================================================

// Connect는 Redis에 연결합니다.
func (a *Adapter) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.connected {
		return nil
	}

	a.client = redis.NewClient(&redis.Options{
		Addr:         a.config.Addr,
		Password:     a.config.Password,
		DB:           a.config.DB,
		PoolSize:     a.config.PoolSize,
		MinIdleConns: a.config.MinIdleConns,
		DialTimeout:  a.config.DialTimeout,
		ReadTimeout:  a.config.ReadTimeout,
		WriteTimeout: a.config.WriteTimeout,
		MaxRetries:   a.config.MaxRetries,
	})

	// 연결 테스트
	if err := a.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis connect failed: %w", err)
	}

	a.connected = true
	return nil
}

// Disconnect는 Redis 연결을 종료합니다.
func (a *Adapter) Disconnect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.connected || a.client == nil {
		return nil
	}

	err := a.client.Close()
	a.connected = false
	return err
}

// IsConnected는 연결 상태를 반환합니다.
func (a *Adapter) IsConnected() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.connected
}

// Ping은 Redis가 응답하는지 확인합니다.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("not connected")
	}

	return client.Ping(ctx).Err()
}

// =============================================================================
// Adapter 인터페이스 구현 - CRUD
// =============================================================================

// prefixKey는 키에 접두사를 붙입니다.
func (a *Adapter) prefixKey(key string) string {
	return a.config.KeyPrefix + key
}

// Get은 키에 해당하는 엔트리를 조회합니다.
func (a *Adapter) Get(ctx context.Context, key string) (*core.Entry, error) {
	start := time.Now()
	defer func() {
		atomic.AddInt64(&a.getLatency, time.Since(start).Nanoseconds())
		atomic.AddUint64(&a.getCount, 1)
	}()

	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}

	data, err := client.Get(ctx, a.prefixKey(key)).Bytes()
	if err == redis.Nil {
		atomic.AddUint64(&a.misses, 1)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get error: %w", err)
	}

	// JSON 역직렬화
	var entry core.Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	atomic.AddUint64(&a.hits, 1)
	return &entry, nil
}

// Set은 엔트리를 저장합니다.
func (a *Adapter) Set(ctx context.Context, entry *core.Entry) error {
	start := time.Now()
	defer func() {
		atomic.AddInt64(&a.setLatency, time.Since(start).Nanoseconds())
		atomic.AddUint64(&a.setCount, 1)
	}()

	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("not connected")
	}

	// JSON 직렬화
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	// TTL 계산
	var ttl time.Duration
	if !entry.ExpiresAt.IsZero() {
		ttl = time.Until(entry.ExpiresAt)
		if ttl <= 0 {
			// 이미 만료됨 - 저장하지 않음
			return nil
		}
	}

	// Redis에 저장 (TTL 포함)
	if ttl > 0 {
		err = client.Set(ctx, a.prefixKey(entry.Key), data, ttl).Err()
	} else {
		err = client.Set(ctx, a.prefixKey(entry.Key), data, 0).Err()
	}

	if err != nil {
		return fmt.Errorf("redis set error: %w", err)
	}

	return nil
}

// Delete는 엔트리를 삭제합니다.
func (a *Adapter) Delete(ctx context.Context, key string) (bool, error) {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return false, fmt.Errorf("not connected")
	}

	result, err := client.Del(ctx, a.prefixKey(key)).Result()
	if err != nil {
		return false, fmt.Errorf("redis del error: %w", err)
	}

	return result > 0, nil
}

// Has는 키가 존재하는지 확인합니다.
func (a *Adapter) Has(ctx context.Context, key string) (bool, error) {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return false, fmt.Errorf("not connected")
	}

	result, err := client.Exists(ctx, a.prefixKey(key)).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists error: %w", err)
	}

	return result > 0, nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 배치 연산
// =============================================================================

// GetMany는 여러 키를 한 번에 조회합니다.
func (a *Adapter) GetMany(ctx context.Context, keys []string) (map[string]*core.Entry, error) {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}

	// 접두사 붙인 키 목록
	prefixedKeys := make([]string, len(keys))
	for i, key := range keys {
		prefixedKeys[i] = a.prefixKey(key)
	}

	// MGET으로 일괄 조회
	values, err := client.MGet(ctx, prefixedKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget error: %w", err)
	}

	result := make(map[string]*core.Entry)
	for i, val := range values {
		if val == nil {
			atomic.AddUint64(&a.misses, 1)
			continue
		}

		data, ok := val.(string)
		if !ok {
			continue
		}

		var entry core.Entry
		if err := json.Unmarshal([]byte(data), &entry); err != nil {
			continue
		}

		result[keys[i]] = &entry
		atomic.AddUint64(&a.hits, 1)
	}

	return result, nil
}

// SetMany는 여러 엔트리를 한 번에 저장합니다.
func (a *Adapter) SetMany(ctx context.Context, entries []*core.Entry) error {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("not connected")
	}

	// 파이프라인 사용
	pipe := client.Pipeline()

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			continue
		}

		var ttl time.Duration
		if !entry.ExpiresAt.IsZero() {
			ttl = time.Until(entry.ExpiresAt)
			if ttl <= 0 {
				continue // 이미 만료됨
			}
		}

		if ttl > 0 {
			pipe.Set(ctx, a.prefixKey(entry.Key), data, ttl)
		} else {
			pipe.Set(ctx, a.prefixKey(entry.Key), data, 0)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline error: %w", err)
	}

	return nil
}

// DeleteMany는 여러 키를 한 번에 삭제합니다.
func (a *Adapter) DeleteMany(ctx context.Context, keys []string) (int, error) {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return 0, fmt.Errorf("not connected")
	}

	prefixedKeys := make([]string, len(keys))
	for i, key := range keys {
		prefixedKeys[i] = a.prefixKey(key)
	}

	result, err := client.Del(ctx, prefixedKeys...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis del error: %w", err)
	}

	return int(result), nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 관리
// =============================================================================

// Keys는 패턴에 매칭되는 모든 키를 반환합니다.
func (a *Adapter) Keys(ctx context.Context, pattern string) ([]string, error) {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return nil, fmt.Errorf("not connected")
	}

	// 패턴에 접두사 추가
	if pattern == "" {
		pattern = "*"
	}
	pattern = a.config.KeyPrefix + pattern

	// SCAN으로 키 조회 (KEYS보다 안전)
	var keys []string
	var cursor uint64
	for {
		var batch []string
		var err error
		batch, cursor, err = client.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return nil, fmt.Errorf("redis scan error: %w", err)
		}

		// 접두사 제거
		for _, key := range batch {
			keys = append(keys, key[len(a.config.KeyPrefix):])
		}

		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

// Clear는 모든 엔트리를 삭제합니다.
func (a *Adapter) Clear(ctx context.Context) error {
	keys, err := a.Keys(ctx, "*")
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	_, err = a.DeleteMany(ctx, keys)
	return err
}

// Size는 저장된 엔트리 개수를 반환합니다.
func (a *Adapter) Size(ctx context.Context) (int64, error) {
	keys, err := a.Keys(ctx, "*")
	if err != nil {
		return 0, err
	}
	return int64(len(keys)), nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 메트릭
// =============================================================================

// Stats는 어댑터 통계를 반환합니다.
func (a *Adapter) Stats(ctx context.Context) (*core.AdapterStats, error) {
	getCount := atomic.LoadUint64(&a.getCount)
	setCount := atomic.LoadUint64(&a.setCount)

	var avgGetLatency, avgSetLatency int64
	if getCount > 0 {
		avgGetLatency = atomic.LoadInt64(&a.getLatency) / int64(getCount)
	}
	if setCount > 0 {
		avgSetLatency = atomic.LoadInt64(&a.setLatency) / int64(setCount)
	}

	size, _ := a.Size(ctx)

	return &core.AdapterStats{
		Name:         a.Name(),
		Type:         a.Type(),
		Connected:    a.IsConnected(),
		Size:         size,
		MaxSize:      0, // Redis는 제한 없음
		Hits:         atomic.LoadUint64(&a.hits),
		Misses:       atomic.LoadUint64(&a.misses),
		GetLatencyNs: avgGetLatency,
		SetLatencyNs: avgSetLatency,
	}, nil
}

// =============================================================================
// TTLAdapter 인터페이스 구현
// =============================================================================

// SetWithTTL은 TTL과 함께 엔트리를 저장합니다.
func (a *Adapter) SetWithTTL(ctx context.Context, entry *core.Entry) error {
	return a.Set(ctx, entry)
}

// GetTTL은 키의 남은 TTL을 반환합니다 (초 단위).
func (a *Adapter) GetTTL(ctx context.Context, key string) (int64, error) {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return 0, fmt.Errorf("not connected")
	}

	ttl, err := client.TTL(ctx, a.prefixKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis ttl error: %w", err)
	}

	if ttl < 0 {
		return -1, nil // 만료 없음 또는 키 없음
	}

	return int64(ttl.Seconds()), nil
}

// SetTTL은 기존 키의 TTL을 변경합니다.
func (a *Adapter) SetTTL(ctx context.Context, key string, ttlSeconds int64) error {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("not connected")
	}

	var err error
	if ttlSeconds > 0 {
		err = client.Expire(ctx, a.prefixKey(key), time.Duration(ttlSeconds)*time.Second).Err()
	} else {
		err = client.Persist(ctx, a.prefixKey(key)).Err() // TTL 제거
	}

	if err != nil {
		return fmt.Errorf("redis expire error: %w", err)
	}

	return nil
}

// =============================================================================
// Redis 전용 기능
// =============================================================================

// Client는 내부 Redis 클라이언트를 반환합니다.
// 고급 기능이 필요할 때 사용합니다.
func (a *Adapter) Client() *redis.Client {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.client
}

// Publish는 채널에 메시지를 발행합니다.
// 캐시 무효화 전파에 사용할 수 있습니다.
func (a *Adapter) Publish(ctx context.Context, channel string, message interface{}) error {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return fmt.Errorf("not connected")
	}

	return client.Publish(ctx, channel, message).Err()
}

// Subscribe는 채널을 구독합니다.
func (a *Adapter) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	a.mu.RLock()
	client := a.client
	a.mu.RUnlock()

	if client == nil {
		return nil
	}

	return client.Subscribe(ctx, channels...)
}
