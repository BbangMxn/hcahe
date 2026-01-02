// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 분산 캐시 무효화를 구현합니다.
package core

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// =============================================================================
// InvalidationMessage: 무효화 메시지
// =============================================================================

// InvalidationType은 무효화 유형입니다.
type InvalidationType int

const (
	InvalidateKey InvalidationType = iota
	InvalidateKeys
	InvalidateTag
	InvalidateTags
	InvalidateAll
)

// InvalidationMessage는 분산 무효화 메시지입니다.
type InvalidationMessage struct {
	Type      InvalidationType `json:"type"`
	Keys      []string         `json:"keys,omitempty"`
	Tags      []string         `json:"tags,omitempty"`
	Source    string           `json:"source"`    // 발신 인스턴스 ID
	Timestamp int64            `json:"timestamp"` // Unix nano
}

// =============================================================================
// InvalidationBroadcaster: 무효화 브로드캐스터 인터페이스
// =============================================================================

// InvalidationBroadcaster는 무효화 메시지를 브로드캐스트합니다.
type InvalidationBroadcaster interface {
	// Publish는 무효화 메시지를 발행합니다.
	Publish(ctx context.Context, msg *InvalidationMessage) error

	// Subscribe는 무효화 메시지를 구독합니다.
	Subscribe(ctx context.Context, handler func(*InvalidationMessage)) error

	// Close는 연결을 종료합니다.
	Close() error
}

// =============================================================================
// InvalidationManager: 무효화 관리자
// =============================================================================

// InvalidationManager는 분산 캐시 무효화를 관리합니다.
type InvalidationManager struct {
	cache       *Cache
	broadcaster InvalidationBroadcaster
	instanceID  string

	// 중복 처리 방지
	processed    map[int64]bool
	processedMu  sync.RWMutex
	maxProcessed int

	closeCh chan struct{}
	closed  bool
	mu      sync.RWMutex
}

// NewInvalidationManager는 새로운 무효화 관리자를 생성합니다.
func NewInvalidationManager(cache *Cache, broadcaster InvalidationBroadcaster, instanceID string) *InvalidationManager {
	return &InvalidationManager{
		cache:        cache,
		broadcaster:  broadcaster,
		instanceID:   instanceID,
		processed:    make(map[int64]bool),
		maxProcessed: 10000,
		closeCh:      make(chan struct{}),
	}
}

// Start는 무효화 메시지 수신을 시작합니다.
func (m *InvalidationManager) Start(ctx context.Context) error {
	return m.broadcaster.Subscribe(ctx, m.handleMessage)
}

// Stop은 무효화 관리자를 종료합니다.
func (m *InvalidationManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true
	close(m.closeCh)

	return m.broadcaster.Close()
}

// InvalidateKey는 키를 무효화하고 다른 인스턴스에 전파합니다.
func (m *InvalidationManager) InvalidateKey(ctx context.Context, key string) error {
	// 로컬 삭제
	m.cache.Delete(ctx, key)

	// 브로드캐스트
	msg := &InvalidationMessage{
		Type:      InvalidateKey,
		Keys:      []string{key},
		Source:    m.instanceID,
		Timestamp: time.Now().UnixNano(),
	}

	return m.broadcaster.Publish(ctx, msg)
}

// InvalidateKeys는 여러 키를 무효화합니다.
func (m *InvalidationManager) InvalidateKeys(ctx context.Context, keys []string) error {
	// 로컬 삭제
	for _, key := range keys {
		m.cache.Delete(ctx, key)
	}

	// 브로드캐스트
	msg := &InvalidationMessage{
		Type:      InvalidateKeys,
		Keys:      keys,
		Source:    m.instanceID,
		Timestamp: time.Now().UnixNano(),
	}

	return m.broadcaster.Publish(ctx, msg)
}

// InvalidateTag는 태그에 해당하는 모든 키를 무효화합니다.
func (m *InvalidationManager) InvalidateTag(ctx context.Context, tag string) error {
	msg := &InvalidationMessage{
		Type:      InvalidateTag,
		Tags:      []string{tag},
		Source:    m.instanceID,
		Timestamp: time.Now().UnixNano(),
	}

	return m.broadcaster.Publish(ctx, msg)
}

// InvalidateAll은 모든 캐시를 무효화합니다.
func (m *InvalidationManager) InvalidateAll(ctx context.Context) error {
	// 로컬 클리어
	m.cache.Clear(ctx)

	// 브로드캐스트
	msg := &InvalidationMessage{
		Type:      InvalidateAll,
		Source:    m.instanceID,
		Timestamp: time.Now().UnixNano(),
	}

	return m.broadcaster.Publish(ctx, msg)
}

// handleMessage는 수신된 무효화 메시지를 처리합니다.
func (m *InvalidationManager) handleMessage(msg *InvalidationMessage) {
	// 자신이 보낸 메시지 무시
	if msg.Source == m.instanceID {
		return
	}

	// 중복 체크
	if m.isProcessed(msg.Timestamp) {
		return
	}
	m.markProcessed(msg.Timestamp)

	ctx := context.Background()

	switch msg.Type {
	case InvalidateKey:
		if len(msg.Keys) > 0 {
			m.cache.Delete(ctx, msg.Keys[0])
		}

	case InvalidateKeys:
		for _, key := range msg.Keys {
			m.cache.Delete(ctx, key)
		}

	case InvalidateTag, InvalidateTags:
		// 태그 기반 무효화는 IntelligentCache에서 처리
		// 여기서는 로컬 L1만 클리어
		layers := m.cache.Layers()
		if len(layers) > 0 {
			layers[0].Adapter().Clear(ctx)
		}

	case InvalidateAll:
		m.cache.Clear(ctx)
	}
}

// isProcessed는 메시지가 이미 처리되었는지 확인합니다.
func (m *InvalidationManager) isProcessed(timestamp int64) bool {
	m.processedMu.RLock()
	defer m.processedMu.RUnlock()
	return m.processed[timestamp]
}

// markProcessed는 메시지를 처리됨으로 표시합니다.
func (m *InvalidationManager) markProcessed(timestamp int64) {
	m.processedMu.Lock()
	defer m.processedMu.Unlock()

	// 오래된 항목 정리
	if len(m.processed) >= m.maxProcessed {
		// 간단히 전체 클리어 (실제로는 오래된 것만 제거해야 함)
		m.processed = make(map[int64]bool)
	}

	m.processed[timestamp] = true
}

// =============================================================================
// JSON Marshaling
// =============================================================================

// MarshalJSON은 InvalidationMessage를 JSON으로 직렬화합니다.
func (msg *InvalidationMessage) MarshalJSON() ([]byte, error) {
	type Alias InvalidationMessage
	return json.Marshal((*Alias)(msg))
}

// UnmarshalJSON은 JSON에서 InvalidationMessage를 역직렬화합니다.
func (msg *InvalidationMessage) UnmarshalJSON(data []byte) error {
	type Alias InvalidationMessage
	return json.Unmarshal(data, (*Alias)(msg))
}
