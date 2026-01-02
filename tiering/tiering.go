// Package tiering은 자동 티어링(승격/강등)을 구현합니다.
package tiering

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bridgify/hcache/core"
)

// =============================================================================
// Tiering Manager: 자동 티어링 관리자
// =============================================================================
// TieringManager는 캐시 엔트리의 접근 패턴을 분석하여
// 자주 사용되는 데이터는 상위 계층으로, 오래된 데이터는 하위 계층으로 이동합니다.
//
// 승격(Promotion): 하위 계층 → 상위 계층
// - 조건: 접근 횟수가 임계값 이상
//
// 강등(Demotion): 상위 계층 → 하위 계층
// - 조건: 유휴 시간이 임계값 이상
// =============================================================================

// Config는 티어링 설정입니다.
type Config struct {
	// Enabled는 티어링 활성화 여부입니다.
	Enabled bool

	// PromotionThreshold는 승격을 위한 최소 접근 횟수입니다.
	// 이 횟수 이상 접근된 엔트리는 상위 계층으로 승격됩니다.
	PromotionThreshold uint64

	// DemotionIdleTime은 강등을 위한 유휴 시간입니다.
	// 이 시간 이상 접근되지 않은 엔트리는 하위 계층으로 강등됩니다.
	DemotionIdleTime time.Duration

	// CheckInterval은 티어링 체크 주기입니다.
	CheckInterval time.Duration

	// BatchSize는 한 번에 처리할 최대 엔트리 수입니다.
	BatchSize int

	// PromotionCooldown은 같은 키의 연속 승격 방지 시간입니다.
	PromotionCooldown time.Duration

	// DemotionCooldown은 같은 키의 연속 강등 방지 시간입니다.
	DemotionCooldown time.Duration
}

// DefaultConfig는 기본 설정을 반환합니다.
func DefaultConfig() *Config {
	return &Config{
		Enabled:            true,
		PromotionThreshold: 5,
		DemotionIdleTime:   5 * time.Minute,
		CheckInterval:      1 * time.Minute,
		BatchSize:          100,
		PromotionCooldown:  1 * time.Minute,
		DemotionCooldown:   1 * time.Minute,
	}
}

// =============================================================================
// TieringManager
// =============================================================================

// TieringManager는 자동 티어링을 관리합니다.
type TieringManager struct {
	config *Config
	layers []*core.Layer

	// 쿨다운 추적
	promotionCooldown map[string]time.Time
	demotionCooldown  map[string]time.Time

	// 메트릭
	promotions uint64
	demotions  uint64

	// 제어
	ticker  *time.Ticker
	closeCh chan struct{}
	doneCh  chan struct{}
	running bool

	mu sync.RWMutex
}

// =============================================================================
// TieringManager 생성자
// =============================================================================

// New는 새로운 TieringManager를 생성합니다.
func New(config *Config, layers []*core.Layer) *TieringManager {
	if config == nil {
		config = DefaultConfig()
	}

	return &TieringManager{
		config:            config,
		layers:            layers,
		promotionCooldown: make(map[string]time.Time),
		demotionCooldown:  make(map[string]time.Time),
		closeCh:           make(chan struct{}),
		doneCh:            make(chan struct{}),
	}
}

// =============================================================================
// TieringManager 제어
// =============================================================================

// Start는 티어링 관리를 시작합니다.
func (m *TieringManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running || !m.config.Enabled {
		return
	}

	m.running = true
	m.ticker = time.NewTicker(m.config.CheckInterval)

	go m.loop()
}

// Stop은 티어링 관리를 중지합니다.
func (m *TieringManager) Stop() {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return
	}
	m.running = false
	m.mu.Unlock()

	close(m.closeCh)
	<-m.doneCh

	if m.ticker != nil {
		m.ticker.Stop()
	}
}

// IsRunning은 실행 상태를 반환합니다.
func (m *TieringManager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// =============================================================================
// TieringManager 메인 루프
// =============================================================================

// loop는 주기적으로 티어링을 수행합니다.
func (m *TieringManager) loop() {
	defer close(m.doneCh)

	for {
		select {
		case <-m.ticker.C:
			m.runCycle()
		case <-m.closeCh:
			return
		}
	}
}

// runCycle은 한 사이클의 티어링을 수행합니다.
func (m *TieringManager) runCycle() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 승격 체크 (하위 계층부터)
	for i := len(m.layers) - 1; i > 0; i-- {
		m.checkPromotion(ctx, i)
	}

	// 강등 체크 (상위 계층부터)
	for i := 0; i < len(m.layers)-1; i++ {
		m.checkDemotion(ctx, i)
	}

	// 쿨다운 정리
	m.cleanupCooldowns()
}

// =============================================================================
// 승격 로직
// =============================================================================

// checkPromotion은 지정된 계층에서 승격 대상을 찾아 승격합니다.
func (m *TieringManager) checkPromotion(ctx context.Context, layerIdx int) {
	if layerIdx >= len(m.layers) || layerIdx <= 0 {
		return
	}

	layer := m.layers[layerIdx]

	// 이 계층의 모든 키 조회
	keys, err := layer.Adapter().Keys(ctx, "*")
	if err != nil {
		return
	}

	promoted := 0
	for _, key := range keys {
		if promoted >= m.config.BatchSize {
			break
		}

		// 쿨다운 체크
		if !m.canPromote(key) {
			continue
		}

		// 엔트리 조회
		entry, hit, err := layer.Get(ctx, key)
		if err != nil || !hit {
			continue
		}

		// 승격 조건 체크
		if entry.AccessCount >= m.config.PromotionThreshold {
			// 상위 계층으로 승격
			if err := m.promote(ctx, entry, layerIdx); err == nil {
				promoted++
				atomic.AddUint64(&m.promotions, 1)
			}
		}
	}
}

// promote는 엔트리를 상위 계층으로 승격합니다.
func (m *TieringManager) promote(ctx context.Context, entry *core.Entry, fromLayer int) error {
	// 모든 상위 계층에 저장
	for i := 0; i < fromLayer; i++ {
		if err := m.layers[i].Set(ctx, entry.Clone()); err != nil {
			return err
		}
	}

	// 쿨다운 설정
	m.mu.Lock()
	m.promotionCooldown[entry.Key] = time.Now()
	m.mu.Unlock()

	return nil
}

// canPromote는 키가 승격 가능한지 확인합니다.
func (m *TieringManager) canPromote(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if lastTime, exists := m.promotionCooldown[key]; exists {
		return time.Since(lastTime) > m.config.PromotionCooldown
	}
	return true
}

// =============================================================================
// 강등 로직
// =============================================================================

// checkDemotion은 지정된 계층에서 강등 대상을 찾아 강등합니다.
func (m *TieringManager) checkDemotion(ctx context.Context, layerIdx int) {
	if layerIdx >= len(m.layers)-1 {
		return
	}

	layer := m.layers[layerIdx]

	// 이 계층의 모든 키 조회
	keys, err := layer.Adapter().Keys(ctx, "*")
	if err != nil {
		return
	}

	demoted := 0
	for _, key := range keys {
		if demoted >= m.config.BatchSize {
			break
		}

		// 쿨다운 체크
		if !m.canDemote(key) {
			continue
		}

		// 엔트리 조회 (순서 변경 없이)
		entry, err := layer.Adapter().Get(ctx, key)
		if err != nil || entry == nil {
			continue
		}

		// 강등 조건 체크
		if entry.IdleTime() >= m.config.DemotionIdleTime {
			// 하위 계층으로 강등
			if err := m.demote(ctx, entry, layerIdx); err == nil {
				demoted++
				atomic.AddUint64(&m.demotions, 1)
			}
		}
	}
}

// demote는 엔트리를 하위 계층으로 강등합니다.
func (m *TieringManager) demote(ctx context.Context, entry *core.Entry, fromLayer int) error {
	// 바로 아래 계층에 저장
	nextLayer := fromLayer + 1
	if nextLayer >= len(m.layers) {
		return nil
	}

	if err := m.layers[nextLayer].Set(ctx, entry.Clone()); err != nil {
		return err
	}

	// 현재 계층에서 삭제
	if err := m.layers[fromLayer].Delete(ctx, entry.Key); err != nil {
		return err
	}

	// 쿨다운 설정
	m.mu.Lock()
	m.demotionCooldown[entry.Key] = time.Now()
	m.mu.Unlock()

	return nil
}

// canDemote는 키가 강등 가능한지 확인합니다.
func (m *TieringManager) canDemote(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if lastTime, exists := m.demotionCooldown[key]; exists {
		return time.Since(lastTime) > m.config.DemotionCooldown
	}
	return true
}

// =============================================================================
// 유틸리티
// =============================================================================

// cleanupCooldowns는 만료된 쿨다운을 정리합니다.
func (m *TieringManager) cleanupCooldowns() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	for key, lastTime := range m.promotionCooldown {
		if now.Sub(lastTime) > m.config.PromotionCooldown*2 {
			delete(m.promotionCooldown, key)
		}
	}

	for key, lastTime := range m.demotionCooldown {
		if now.Sub(lastTime) > m.config.DemotionCooldown*2 {
			delete(m.demotionCooldown, key)
		}
	}
}

// =============================================================================
// TieringManager 설정 변경
// =============================================================================

// SetConfig는 설정을 변경합니다.
func (m *TieringManager) SetConfig(config *Config) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wasRunning := m.running

	if wasRunning {
		// 일시 중지
		if m.ticker != nil {
			m.ticker.Stop()
		}
	}

	m.config = config

	if wasRunning && config.Enabled {
		// 새 주기로 재시작
		m.ticker = time.NewTicker(config.CheckInterval)
	}
}

// SetLayers는 계층을 업데이트합니다.
func (m *TieringManager) SetLayers(layers []*core.Layer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.layers = layers
}

// =============================================================================
// TieringManager 메트릭
// =============================================================================

// Stats는 티어링 통계를 반환합니다.
type Stats struct {
	Promotions uint64 `json:"promotions"`
	Demotions  uint64 `json:"demotions"`
	Running    bool   `json:"running"`
}

// Stats는 현재 통계를 반환합니다.
func (m *TieringManager) Stats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Stats{
		Promotions: atomic.LoadUint64(&m.promotions),
		Demotions:  atomic.LoadUint64(&m.demotions),
		Running:    m.running,
	}
}

// ResetStats는 통계를 초기화합니다.
func (m *TieringManager) ResetStats() {
	atomic.StoreUint64(&m.promotions, 0)
	atomic.StoreUint64(&m.demotions, 0)
}

// =============================================================================
// 수동 티어링
// =============================================================================

// PromoteKey는 특정 키를 수동으로 승격합니다.
func (m *TieringManager) PromoteKey(ctx context.Context, key string, fromLayer int) error {
	if fromLayer >= len(m.layers) || fromLayer <= 0 {
		return nil
	}

	entry, hit, err := m.layers[fromLayer].Get(ctx, key)
	if err != nil || !hit {
		return err
	}

	return m.promote(ctx, entry, fromLayer)
}

// DemoteKey는 특정 키를 수동으로 강등합니다.
func (m *TieringManager) DemoteKey(ctx context.Context, key string, fromLayer int) error {
	if fromLayer >= len(m.layers)-1 {
		return nil
	}

	entry, err := m.layers[fromLayer].Adapter().Get(ctx, key)
	if err != nil || entry == nil {
		return err
	}

	return m.demote(ctx, entry, fromLayer)
}

// WarmUp은 지정된 키들을 최상위 계층으로 올립니다.
func (m *TieringManager) WarmUp(ctx context.Context, keys []string) error {
	for _, key := range keys {
		// 어느 계층에 있는지 찾기
		for i := len(m.layers) - 1; i > 0; i-- {
			entry, hit, err := m.layers[i].Get(ctx, key)
			if err == nil && hit {
				// 최상위 계층으로 승격
				if err := m.layers[0].Set(ctx, entry.Clone()); err == nil {
					atomic.AddUint64(&m.promotions, 1)
				}
				break
			}
		}
	}
	return nil
}
