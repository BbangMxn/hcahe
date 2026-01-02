// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Local + Remote Cache 동기화를 구현합니다.
package core

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Cache Sync: 로컬과 원격 캐시 동기화
// =============================================================================
// 분산 환경에서 로컬 캐시와 원격 캐시(Redis 등)의 일관성을 유지합니다.
//
// 전략:
// - Pub/Sub 기반 무효화 전파
// - 버전 기반 일관성 체크
// - Write-Through with Invalidation
// =============================================================================

// SyncedCache는 동기화되는 2단계 캐시입니다.
type SyncedCache struct {
	local  *Cache // L1: 로컬 메모리
	remote *Cache // L2: Redis 등

	broadcaster SyncBroadcaster
	instanceID  string // 이 인스턴스 식별자

	// 버전 관리
	versions sync.Map // key -> version

	// 설정
	config *SyncConfig

	// 통계
	syncEvents       uint64
	versionConflicts uint64

	done chan struct{}
}

// SyncBroadcaster는 동기화 메시지를 브로드캐스트하는 인터페이스입니다.
type SyncBroadcaster interface {
	// Publish는 동기화 메시지를 발행합니다.
	Publish(ctx context.Context, msg *SyncMessage) error

	// Subscribe는 동기화 메시지를 구독합니다.
	Subscribe(ctx context.Context, handler func(*SyncMessage)) error

	// Close는 브로드캐스터를 종료합니다.
	Close() error
}

// SyncMessage는 동기화 메시지입니다.
type SyncMessage struct {
	Type       SyncMessageType `json:"type"`
	Key        string          `json:"key"`
	Keys       []string        `json:"keys,omitempty"`
	Version    int64           `json:"version"`
	InstanceID string          `json:"instance_id"`
	Timestamp  int64           `json:"timestamp"`
	Data       []byte          `json:"data,omitempty"`
}

// SyncMessageType은 동기화 메시지 타입입니다.
type SyncMessageType int

const (
	// SyncInvalidate는 캐시 무효화 메시지입니다.
	SyncInvalidate SyncMessageType = iota

	// SyncUpdate는 캐시 업데이트 메시지입니다.
	SyncUpdate

	// SyncBulkInvalidate는 다수 키 무효화 메시지입니다.
	SyncBulkInvalidate
)

// SyncConfig는 동기화 설정입니다.
type SyncConfig struct {
	// InstanceID는 이 인스턴스의 고유 식별자입니다.
	// 빈 문자열이면 자동 생성됩니다.
	InstanceID string

	// LocalTTL은 로컬 캐시 TTL입니다.
	LocalTTL time.Duration

	// VersionCheckEnabled는 버전 체크 활성화 여부입니다.
	VersionCheckEnabled bool

	// PropagateUpdates는 업데이트 데이터도 전파할지 여부입니다.
	// true면 다른 인스턴스도 데이터를 받아 로컬에 저장합니다.
	PropagateUpdates bool
}

// DefaultSyncConfig는 기본 설정을 반환합니다.
func DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		LocalTTL:            30 * time.Second,
		VersionCheckEnabled: true,
		PropagateUpdates:    false,
	}
}

// NewSyncedCache는 새로운 동기화 캐시를 생성합니다.
func NewSyncedCache(local, remote *Cache, broadcaster SyncBroadcaster, config *SyncConfig) *SyncedCache {
	if config == nil {
		config = DefaultSyncConfig()
	}

	instanceID := config.InstanceID
	if instanceID == "" {
		instanceID = generateInstanceID()
	}

	sc := &SyncedCache{
		local:       local,
		remote:      remote,
		broadcaster: broadcaster,
		instanceID:  instanceID,
		config:      config,
		done:        make(chan struct{}),
	}

	// 메시지 구독 시작
	go sc.startSubscription()

	return sc
}

func generateInstanceID() string {
	return time.Now().Format("20060102150405.000000")
}

func (sc *SyncedCache) startSubscription() {
	ctx := context.Background()

	err := sc.broadcaster.Subscribe(ctx, func(msg *SyncMessage) {
		// 자기 자신이 보낸 메시지는 무시
		if msg.InstanceID == sc.instanceID {
			return
		}

		sc.handleSyncMessage(msg)
	})

	if err != nil {
		// 구독 실패 처리
	}
}

func (sc *SyncedCache) handleSyncMessage(msg *SyncMessage) {
	atomic.AddUint64(&sc.syncEvents, 1)

	ctx := context.Background()

	switch msg.Type {
	case SyncInvalidate:
		// 로컬 캐시 무효화
		sc.local.Delete(ctx, msg.Key)
		sc.versions.Delete(msg.Key)

	case SyncUpdate:
		// 버전 체크
		if sc.config.VersionCheckEnabled {
			if currentVer, ok := sc.versions.Load(msg.Key); ok {
				if currentVer.(int64) >= msg.Version {
					atomic.AddUint64(&sc.versionConflicts, 1)
					return // 이미 최신 버전
				}
			}
		}

		// 업데이트 전파 활성화시 로컬에도 저장
		if sc.config.PropagateUpdates && msg.Data != nil {
			entry := NewEntry(msg.Key, msg.Data, sc.config.LocalTTL)
			sc.local.SetRaw(ctx, entry)
		} else {
			// 아니면 무효화만
			sc.local.Delete(ctx, msg.Key)
		}

		sc.versions.Store(msg.Key, msg.Version)

	case SyncBulkInvalidate:
		for _, key := range msg.Keys {
			sc.local.Delete(ctx, key)
			sc.versions.Delete(key)
		}
	}
}

// Get은 로컬 → 원격 순으로 조회합니다.
func (sc *SyncedCache) Get(ctx context.Context, key string, dest interface{}) (int, error) {
	// 1. 로컬 캐시 조회
	layer, err := sc.local.Get(ctx, key, dest)
	if layer >= 0 {
		return 0, err // L1 히트
	}

	// 2. 원격 캐시 조회
	layer, err = sc.remote.Get(ctx, key, dest)
	if layer >= 0 {
		// 로컬에 복제
		entry, _, _ := sc.remote.GetRaw(ctx, key)
		if entry != nil {
			localEntry := entry.Clone()
			localEntry.ExpiresAt = time.Now().Add(sc.config.LocalTTL)
			sc.local.SetRaw(ctx, localEntry)
		}
		return 1, err // L2 히트
	}

	return -1, err
}

// Set은 원격에 저장하고 다른 인스턴스에 알립니다.
func (sc *SyncedCache) Set(ctx context.Context, key string, value interface{}, opts ...SetOption) error {
	// 1. 원격에 저장
	if err := sc.remote.Set(ctx, key, value, opts...); err != nil {
		return err
	}

	// 2. 로컬에도 저장
	localOpts := append(opts, WithTTL(sc.config.LocalTTL))
	sc.local.Set(ctx, key, value, localOpts...)

	// 3. 버전 업데이트
	version := time.Now().UnixNano()
	sc.versions.Store(key, version)

	// 4. 다른 인스턴스에 알림
	msg := &SyncMessage{
		Type:       SyncInvalidate,
		Key:        key,
		Version:    version,
		InstanceID: sc.instanceID,
		Timestamp:  time.Now().Unix(),
	}

	if sc.config.PropagateUpdates {
		msg.Type = SyncUpdate
		// value를 바이트로 변환 필요
		if data, ok := value.([]byte); ok {
			msg.Data = data
		}
	}

	sc.broadcaster.Publish(ctx, msg)

	return nil
}

// Delete는 모든 캐시에서 삭제하고 다른 인스턴스에 알립니다.
func (sc *SyncedCache) Delete(ctx context.Context, key string) error {
	// 1. 로컬 삭제
	sc.local.Delete(ctx, key)
	sc.versions.Delete(key)

	// 2. 원격 삭제
	sc.remote.Delete(ctx, key)

	// 3. 다른 인스턴스에 알림
	msg := &SyncMessage{
		Type:       SyncInvalidate,
		Key:        key,
		InstanceID: sc.instanceID,
		Timestamp:  time.Now().Unix(),
	}

	return sc.broadcaster.Publish(ctx, msg)
}

// InvalidateMany는 여러 키를 무효화합니다.
func (sc *SyncedCache) InvalidateMany(ctx context.Context, keys []string) error {
	for _, key := range keys {
		sc.local.Delete(ctx, key)
		sc.remote.Delete(ctx, key)
		sc.versions.Delete(key)
	}

	msg := &SyncMessage{
		Type:       SyncBulkInvalidate,
		Keys:       keys,
		InstanceID: sc.instanceID,
		Timestamp:  time.Now().Unix(),
	}

	return sc.broadcaster.Publish(ctx, msg)
}

// Stats는 통계를 반환합니다.
func (sc *SyncedCache) Stats() (syncEvents, conflicts uint64) {
	return atomic.LoadUint64(&sc.syncEvents),
		atomic.LoadUint64(&sc.versionConflicts)
}

// Close는 동기화 캐시를 종료합니다.
func (sc *SyncedCache) Close() error {
	close(sc.done)
	return sc.broadcaster.Close()
}

// =============================================================================
// Redis Sync Broadcaster: Redis Pub/Sub 기반 브로드캐스터
// =============================================================================

// RedisSyncBroadcaster는 Redis Pub/Sub 기반 브로드캐스터입니다.
// 실제 구현은 redis 어댑터 패키지에서 합니다.
type RedisSyncBroadcaster struct {
	channel   string
	publish   func(ctx context.Context, channel string, message []byte) error
	subscribe func(ctx context.Context, channel string, handler func([]byte)) error
	close     func() error
}

// NewRedisSyncBroadcaster는 새로운 Redis 브로드캐스터를 생성합니다.
func NewRedisSyncBroadcaster(
	channel string,
	publish func(ctx context.Context, channel string, message []byte) error,
	subscribe func(ctx context.Context, channel string, handler func([]byte)) error,
	close func() error,
) *RedisSyncBroadcaster {
	return &RedisSyncBroadcaster{
		channel:   channel,
		publish:   publish,
		subscribe: subscribe,
		close:     close,
	}
}

func (rb *RedisSyncBroadcaster) Publish(ctx context.Context, msg *SyncMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return rb.publish(ctx, rb.channel, data)
}

func (rb *RedisSyncBroadcaster) Subscribe(ctx context.Context, handler func(*SyncMessage)) error {
	return rb.subscribe(ctx, rb.channel, func(data []byte) {
		var msg SyncMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		handler(&msg)
	})
}

func (rb *RedisSyncBroadcaster) Close() error {
	if rb.close != nil {
		return rb.close()
	}
	return nil
}

// =============================================================================
// Consistent Hash Ring: 샤딩용 일관된 해시
// =============================================================================

// ConsistentHash는 일관된 해시 링입니다.
type ConsistentHash struct {
	ring     []uint32
	nodes    map[uint32]string
	replicas int
	mu       sync.RWMutex
}

// NewConsistentHash는 새로운 일관된 해시를 생성합니다.
func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		nodes:    make(map[uint32]string),
		replicas: replicas,
	}
}

// Add는 노드를 추가합니다.
func (ch *ConsistentHash) Add(node string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	for i := 0; i < ch.replicas; i++ {
		key := hashKey(node + ":" + string(rune(i)))
		ch.ring = append(ch.ring, key)
		ch.nodes[key] = node
	}

	// 정렬
	sortUint32(ch.ring)
}

// Get은 키에 해당하는 노드를 반환합니다.
func (ch *ConsistentHash) Get(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return ""
	}

	hash := hashKey(key)

	// 이진 검색
	idx := searchUint32(ch.ring, hash)
	if idx >= len(ch.ring) {
		idx = 0
	}

	return ch.nodes[ch.ring[idx]]
}

func hashKey(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

func sortUint32(a []uint32) {
	// 간단한 정렬
	for i := 0; i < len(a); i++ {
		for j := i + 1; j < len(a); j++ {
			if a[i] > a[j] {
				a[i], a[j] = a[j], a[i]
			}
		}
	}
}

func searchUint32(a []uint32, x uint32) int {
	lo, hi := 0, len(a)
	for lo < hi {
		mid := (lo + hi) / 2
		if a[mid] < x {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
