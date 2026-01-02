// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Circuit Breaker 패턴을 구현합니다.
package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Circuit Breaker: 장애 자동 우회
// =============================================================================
// 백엔드(Redis, PostgreSQL 등) 장애 시 자동으로 우회하여
// 전체 시스템이 다운되는 것을 방지합니다.
//
// 상태:
// - Closed: 정상 동작, 요청 통과
// - Open: 장애 상태, 요청 즉시 실패
// - HalfOpen: 복구 테스트 중, 일부 요청만 통과
// =============================================================================

// CircuitState는 서킷 브레이커 상태입니다.
type CircuitState int32

const (
	StateClosed   CircuitState = iota // 정상
	StateOpen                         // 차단
	StateHalfOpen                     // 복구 테스트 중
)

func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig는 서킷 브레이커 설정입니다.
type CircuitBreakerConfig struct {
	// FailureThreshold는 Open 상태로 전환하기 위한 연속 실패 횟수입니다.
	FailureThreshold int

	// SuccessThreshold는 Closed 상태로 복구하기 위한 연속 성공 횟수입니다.
	SuccessThreshold int

	// Timeout은 Open 상태에서 HalfOpen으로 전환되기까지의 시간입니다.
	Timeout time.Duration

	// MaxHalfOpenRequests는 HalfOpen 상태에서 허용되는 최대 동시 요청 수입니다.
	MaxHalfOpenRequests int

	// OnStateChange는 상태 변경 시 호출되는 콜백입니다.
	OnStateChange func(from, to CircuitState)
}

// DefaultCircuitBreakerConfig는 기본 설정을 반환합니다.
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		FailureThreshold:    5,
		SuccessThreshold:    3,
		Timeout:             30 * time.Second,
		MaxHalfOpenRequests: 1,
		OnStateChange:       nil,
	}
}

// CircuitBreaker는 서킷 브레이커입니다.
type CircuitBreaker struct {
	config *CircuitBreakerConfig

	state            int32 // atomic
	failures         int32 // atomic
	successes        int32 // atomic
	lastFailureTime  int64 // atomic, unix nano
	halfOpenRequests int32 // atomic

	mu sync.RWMutex
}

// NewCircuitBreaker는 새로운 서킷 브레이커를 생성합니다.
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}

	return &CircuitBreaker{
		config: config,
		state:  int32(StateClosed),
	}
}

// State는 현재 상태를 반환합니다.
func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(atomic.LoadInt32(&cb.state))
}

// Allow는 요청을 허용할지 결정합니다.
func (cb *CircuitBreaker) Allow() bool {
	state := cb.State()

	switch state {
	case StateClosed:
		return true

	case StateOpen:
		// Timeout이 지났으면 HalfOpen으로 전환
		lastFailure := atomic.LoadInt64(&cb.lastFailureTime)
		if time.Since(time.Unix(0, lastFailure)) > cb.config.Timeout {
			cb.transitionTo(StateHalfOpen)
			return cb.tryHalfOpenRequest()
		}
		return false

	case StateHalfOpen:
		return cb.tryHalfOpenRequest()

	default:
		return true
	}
}

// tryHalfOpenRequest는 HalfOpen 상태에서 요청을 시도합니다.
func (cb *CircuitBreaker) tryHalfOpenRequest() bool {
	current := atomic.AddInt32(&cb.halfOpenRequests, 1)
	if int(current) <= cb.config.MaxHalfOpenRequests {
		return true
	}
	atomic.AddInt32(&cb.halfOpenRequests, -1)
	return false
}

// RecordSuccess는 성공을 기록합니다.
func (cb *CircuitBreaker) RecordSuccess() {
	state := cb.State()

	switch state {
	case StateClosed:
		atomic.StoreInt32(&cb.failures, 0)

	case StateHalfOpen:
		atomic.AddInt32(&cb.halfOpenRequests, -1)
		successes := atomic.AddInt32(&cb.successes, 1)

		if int(successes) >= cb.config.SuccessThreshold {
			cb.transitionTo(StateClosed)
		}
	}
}

// RecordFailure는 실패를 기록합니다.
func (cb *CircuitBreaker) RecordFailure() {
	atomic.StoreInt64(&cb.lastFailureTime, time.Now().UnixNano())

	state := cb.State()

	switch state {
	case StateClosed:
		failures := atomic.AddInt32(&cb.failures, 1)

		if int(failures) >= cb.config.FailureThreshold {
			cb.transitionTo(StateOpen)
		}

	case StateHalfOpen:
		atomic.AddInt32(&cb.halfOpenRequests, -1)
		cb.transitionTo(StateOpen)
	}
}

// transitionTo는 상태를 전환합니다.
func (cb *CircuitBreaker) transitionTo(newState CircuitState) {
	oldState := CircuitState(atomic.SwapInt32(&cb.state, int32(newState)))

	if oldState == newState {
		return
	}

	// 상태별 초기화
	switch newState {
	case StateClosed:
		atomic.StoreInt32(&cb.failures, 0)
		atomic.StoreInt32(&cb.successes, 0)

	case StateOpen:
		atomic.StoreInt32(&cb.successes, 0)

	case StateHalfOpen:
		atomic.StoreInt32(&cb.halfOpenRequests, 0)
		atomic.StoreInt32(&cb.successes, 0)
	}

	// 콜백 호출
	if cb.config.OnStateChange != nil {
		go cb.config.OnStateChange(oldState, newState)
	}
}

// Reset은 서킷 브레이커를 초기 상태로 리셋합니다.
func (cb *CircuitBreaker) Reset() {
	cb.transitionTo(StateClosed)
}

// =============================================================================
// CircuitBreakerError
// =============================================================================

// ErrCircuitOpen은 서킷이 열려있을 때 반환되는 에러입니다.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// =============================================================================
// ResilientAdapter: 서킷 브레이커가 적용된 어댑터
// =============================================================================

// ResilientAdapter는 서킷 브레이커가 적용된 어댑터 래퍼입니다.
type ResilientAdapter struct {
	adapter Adapter
	cb      *CircuitBreaker
}

// NewResilientAdapter는 새로운 ResilientAdapter를 생성합니다.
func NewResilientAdapter(adapter Adapter, config *CircuitBreakerConfig) *ResilientAdapter {
	return &ResilientAdapter{
		adapter: adapter,
		cb:      NewCircuitBreaker(config),
	}
}

// 기본 정보
func (r *ResilientAdapter) Name() string      { return r.adapter.Name() }
func (r *ResilientAdapter) Type() AdapterType { return r.adapter.Type() }

// 연결 관리
func (r *ResilientAdapter) Connect(ctx context.Context) error {
	return r.adapter.Connect(ctx)
}

func (r *ResilientAdapter) Disconnect(ctx context.Context) error {
	return r.adapter.Disconnect(ctx)
}

func (r *ResilientAdapter) IsConnected() bool {
	return r.adapter.IsConnected()
}

func (r *ResilientAdapter) Ping(ctx context.Context) error {
	if !r.cb.Allow() {
		return ErrCircuitOpen
	}

	err := r.adapter.Ping(ctx)
	if err != nil {
		r.cb.RecordFailure()
		return err
	}

	r.cb.RecordSuccess()
	return nil
}

// CRUD with Circuit Breaker
func (r *ResilientAdapter) Get(ctx context.Context, key string) (*Entry, error) {
	if !r.cb.Allow() {
		return nil, ErrCircuitOpen
	}

	entry, err := r.adapter.Get(ctx, key)
	if err != nil {
		r.cb.RecordFailure()
		return nil, err
	}

	r.cb.RecordSuccess()
	return entry, nil
}

func (r *ResilientAdapter) Set(ctx context.Context, entry *Entry) error {
	if !r.cb.Allow() {
		return ErrCircuitOpen
	}

	err := r.adapter.Set(ctx, entry)
	if err != nil {
		r.cb.RecordFailure()
		return err
	}

	r.cb.RecordSuccess()
	return nil
}

func (r *ResilientAdapter) Delete(ctx context.Context, key string) (bool, error) {
	if !r.cb.Allow() {
		return false, ErrCircuitOpen
	}

	deleted, err := r.adapter.Delete(ctx, key)
	if err != nil {
		r.cb.RecordFailure()
		return false, err
	}

	r.cb.RecordSuccess()
	return deleted, nil
}

func (r *ResilientAdapter) Has(ctx context.Context, key string) (bool, error) {
	if !r.cb.Allow() {
		return false, ErrCircuitOpen
	}

	exists, err := r.adapter.Has(ctx, key)
	if err != nil {
		r.cb.RecordFailure()
		return false, err
	}

	r.cb.RecordSuccess()
	return exists, nil
}

// 배치 연산
func (r *ResilientAdapter) GetMany(ctx context.Context, keys []string) (map[string]*Entry, error) {
	if !r.cb.Allow() {
		return nil, ErrCircuitOpen
	}

	entries, err := r.adapter.GetMany(ctx, keys)
	if err != nil {
		r.cb.RecordFailure()
		return nil, err
	}

	r.cb.RecordSuccess()
	return entries, nil
}

func (r *ResilientAdapter) SetMany(ctx context.Context, entries []*Entry) error {
	if !r.cb.Allow() {
		return ErrCircuitOpen
	}

	err := r.adapter.SetMany(ctx, entries)
	if err != nil {
		r.cb.RecordFailure()
		return err
	}

	r.cb.RecordSuccess()
	return nil
}

func (r *ResilientAdapter) DeleteMany(ctx context.Context, keys []string) (int, error) {
	if !r.cb.Allow() {
		return 0, ErrCircuitOpen
	}

	count, err := r.adapter.DeleteMany(ctx, keys)
	if err != nil {
		r.cb.RecordFailure()
		return 0, err
	}

	r.cb.RecordSuccess()
	return count, nil
}

// 관리
func (r *ResilientAdapter) Keys(ctx context.Context, pattern string) ([]string, error) {
	if !r.cb.Allow() {
		return nil, ErrCircuitOpen
	}

	keys, err := r.adapter.Keys(ctx, pattern)
	if err != nil {
		r.cb.RecordFailure()
		return nil, err
	}

	r.cb.RecordSuccess()
	return keys, nil
}

func (r *ResilientAdapter) Clear(ctx context.Context) error {
	if !r.cb.Allow() {
		return ErrCircuitOpen
	}

	err := r.adapter.Clear(ctx)
	if err != nil {
		r.cb.RecordFailure()
		return err
	}

	r.cb.RecordSuccess()
	return nil
}

func (r *ResilientAdapter) Size(ctx context.Context) (int64, error) {
	if !r.cb.Allow() {
		return 0, ErrCircuitOpen
	}

	size, err := r.adapter.Size(ctx)
	if err != nil {
		r.cb.RecordFailure()
		return 0, err
	}

	r.cb.RecordSuccess()
	return size, nil
}

func (r *ResilientAdapter) Stats(ctx context.Context) (*AdapterStats, error) {
	stats, err := r.adapter.Stats(ctx)
	if err != nil {
		return nil, err
	}

	// 서킷 브레이커 상태 추가
	stats.CircuitState = r.cb.State().String()
	return stats, nil
}

// CircuitBreaker는 내부 서킷 브레이커를 반환합니다.
func (r *ResilientAdapter) CircuitBreaker() *CircuitBreaker {
	return r.cb
}

// Unwrap은 원본 어댑터를 반환합니다.
func (r *ResilientAdapter) Unwrap() Adapter {
	return r.adapter
}
