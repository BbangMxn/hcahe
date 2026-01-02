// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Rate Limiter를 구현합니다.
package core

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Rate Limiter: 요청 제한
// =============================================================================
// Token Bucket과 Sliding Window 알고리즘을 지원합니다.
// 캐시와 통합하여 키별/전역 요청 제한이 가능합니다.
// =============================================================================

// RateLimiter는 요청 제한 인터페이스입니다.
type RateLimiter interface {
	// Allow는 요청이 허용되는지 확인합니다.
	Allow(key string) bool

	// AllowN은 N개의 요청이 허용되는지 확인합니다.
	AllowN(key string, n int) bool

	// Wait는 요청이 허용될 때까지 대기합니다.
	Wait(ctx context.Context, key string) error

	// Reset은 키의 제한을 초기화합니다.
	Reset(key string)
}

// =============================================================================
// Token Bucket Rate Limiter
// =============================================================================

// TokenBucketLimiter는 토큰 버킷 알고리즘 기반 제한기입니다.
type TokenBucketLimiter struct {
	rate         float64      // 초당 토큰 생성률
	burst        int          // 최대 버스트 크기
	buckets      sync.Map     // key -> *tokenBucket
	globalBucket *tokenBucket // 전역 버킷 (nil이면 키별만 사용)
}

type tokenBucket struct {
	tokens     float64
	lastUpdate int64 // unix nano
	mu         sync.Mutex
}

// TokenBucketConfig는 토큰 버킷 설정입니다.
type TokenBucketConfig struct {
	Rate        float64 // 초당 토큰 생성률
	Burst       int     // 최대 버스트 크기
	GlobalRate  float64 // 전역 제한 (0이면 비활성화)
	GlobalBurst int     // 전역 버스트
}

// DefaultTokenBucketConfig는 기본 설정을 반환합니다.
func DefaultTokenBucketConfig() *TokenBucketConfig {
	return &TokenBucketConfig{
		Rate:  100, // 초당 100개
		Burst: 200, // 최대 200개 버스트
	}
}

// NewTokenBucketLimiter는 새로운 토큰 버킷 제한기를 생성합니다.
func NewTokenBucketLimiter(config *TokenBucketConfig) *TokenBucketLimiter {
	if config == nil {
		config = DefaultTokenBucketConfig()
	}

	limiter := &TokenBucketLimiter{
		rate:  config.Rate,
		burst: config.Burst,
	}

	if config.GlobalRate > 0 {
		limiter.globalBucket = &tokenBucket{
			tokens:     float64(config.GlobalBurst),
			lastUpdate: time.Now().UnixNano(),
		}
	}

	return limiter
}

func (l *TokenBucketLimiter) getBucket(key string) *tokenBucket {
	if bucket, ok := l.buckets.Load(key); ok {
		return bucket.(*tokenBucket)
	}

	newBucket := &tokenBucket{
		tokens:     float64(l.burst),
		lastUpdate: time.Now().UnixNano(),
	}

	actual, _ := l.buckets.LoadOrStore(key, newBucket)
	return actual.(*tokenBucket)
}

func (b *tokenBucket) take(rate float64, burst int, n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now().UnixNano()
	elapsed := float64(now-b.lastUpdate) / float64(time.Second)
	b.lastUpdate = now

	// 토큰 보충
	b.tokens += elapsed * rate
	if b.tokens > float64(burst) {
		b.tokens = float64(burst)
	}

	// 토큰 소비
	if b.tokens >= float64(n) {
		b.tokens -= float64(n)
		return true
	}

	return false
}

// Allow는 요청이 허용되는지 확인합니다.
func (l *TokenBucketLimiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN은 N개의 요청이 허용되는지 확인합니다.
func (l *TokenBucketLimiter) AllowN(key string, n int) bool {
	// 전역 제한 확인
	if l.globalBucket != nil {
		if !l.globalBucket.take(l.rate*10, l.burst*10, n) {
			return false
		}
	}

	// 키별 제한 확인
	bucket := l.getBucket(key)
	return bucket.take(l.rate, l.burst, n)
}

// Wait는 요청이 허용될 때까지 대기합니다.
func (l *TokenBucketLimiter) Wait(ctx context.Context, key string) error {
	for {
		if l.Allow(key) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 10):
			// 재시도
		}
	}
}

// Reset은 키의 제한을 초기화합니다.
func (l *TokenBucketLimiter) Reset(key string) {
	l.buckets.Delete(key)
}

// =============================================================================
// Sliding Window Rate Limiter
// =============================================================================

// SlidingWindowLimiter는 슬라이딩 윈도우 알고리즘 기반 제한기입니다.
type SlidingWindowLimiter struct {
	limit    int           // 윈도우당 최대 요청 수
	window   time.Duration // 윈도우 크기
	counters sync.Map      // key -> *slidingWindow
}

type slidingWindow struct {
	prevCount   int64
	currCount   int64
	windowStart int64 // unix nano
	mu          sync.Mutex
}

// SlidingWindowConfig는 슬라이딩 윈도우 설정입니다.
type SlidingWindowConfig struct {
	Limit  int           // 윈도우당 최대 요청 수
	Window time.Duration // 윈도우 크기
}

// DefaultSlidingWindowConfig는 기본 설정을 반환합니다.
func DefaultSlidingWindowConfig() *SlidingWindowConfig {
	return &SlidingWindowConfig{
		Limit:  1000,
		Window: time.Minute,
	}
}

// NewSlidingWindowLimiter는 새로운 슬라이딩 윈도우 제한기를 생성합니다.
func NewSlidingWindowLimiter(config *SlidingWindowConfig) *SlidingWindowLimiter {
	if config == nil {
		config = DefaultSlidingWindowConfig()
	}

	return &SlidingWindowLimiter{
		limit:  config.Limit,
		window: config.Window,
	}
}

func (l *SlidingWindowLimiter) getWindow(key string) *slidingWindow {
	if w, ok := l.counters.Load(key); ok {
		return w.(*slidingWindow)
	}

	newWindow := &slidingWindow{
		windowStart: time.Now().UnixNano(),
	}

	actual, _ := l.counters.LoadOrStore(key, newWindow)
	return actual.(*slidingWindow)
}

// Allow는 요청이 허용되는지 확인합니다.
func (l *SlidingWindowLimiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN은 N개의 요청이 허용되는지 확인합니다.
func (l *SlidingWindowLimiter) AllowN(key string, n int) bool {
	w := l.getWindow(key)

	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now().UnixNano()
	windowNano := l.window.Nanoseconds()

	// 현재 윈도우 확인
	elapsed := now - w.windowStart
	if elapsed >= windowNano*2 {
		// 2개 윈도우 이상 지남 - 리셋
		w.prevCount = 0
		w.currCount = 0
		w.windowStart = now
	} else if elapsed >= windowNano {
		// 1개 윈도우 지남 - 롤오버
		w.prevCount = w.currCount
		w.currCount = 0
		w.windowStart += windowNano
		elapsed = now - w.windowStart
	}

	// 슬라이딩 윈도우 계산
	prevWeight := 1.0 - float64(elapsed)/float64(windowNano)
	if prevWeight < 0 {
		prevWeight = 0
	}

	currentCount := float64(w.prevCount)*prevWeight + float64(w.currCount)

	if int(currentCount)+n > l.limit {
		return false
	}

	w.currCount += int64(n)
	return true
}

// Wait는 요청이 허용될 때까지 대기합니다.
func (l *SlidingWindowLimiter) Wait(ctx context.Context, key string) error {
	for {
		if l.Allow(key) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 50):
			// 재시도
		}
	}
}

// Reset은 키의 제한을 초기화합니다.
func (l *SlidingWindowLimiter) Reset(key string) {
	l.counters.Delete(key)
}

// =============================================================================
// Rate Limited Cache: Rate Limiter가 통합된 캐시
// =============================================================================

// RateLimitedCache는 Rate Limiter가 통합된 캐시입니다.
type RateLimitedCache struct {
	cache   *Cache
	limiter RateLimiter

	// 통계
	allowed  uint64
	rejected uint64
}

// RateLimitedCacheConfig는 설정입니다.
type RateLimitedCacheConfig struct {
	Limiter RateLimiter
}

// NewRateLimitedCache는 새로운 Rate Limited 캐시를 생성합니다.
func NewRateLimitedCache(cache *Cache, limiter RateLimiter) *RateLimitedCache {
	return &RateLimitedCache{
		cache:   cache,
		limiter: limiter,
	}
}

// Get은 Rate Limit을 확인한 후 캐시를 조회합니다.
func (c *RateLimitedCache) Get(ctx context.Context, key string, dest interface{}) (int, error) {
	if !c.limiter.Allow(key) {
		atomic.AddUint64(&c.rejected, 1)
		return -1, ErrRateLimited
	}

	atomic.AddUint64(&c.allowed, 1)
	return c.cache.Get(ctx, key, dest)
}

// Set은 Rate Limit을 확인한 후 캐시에 저장합니다.
func (c *RateLimitedCache) Set(ctx context.Context, key string, value interface{}, opts ...SetOption) error {
	if !c.limiter.Allow(key) {
		atomic.AddUint64(&c.rejected, 1)
		return ErrRateLimited
	}

	atomic.AddUint64(&c.allowed, 1)
	return c.cache.Set(ctx, key, value, opts...)
}

// Stats는 Rate Limit 통계를 반환합니다.
func (c *RateLimitedCache) Stats() (allowed, rejected uint64) {
	return atomic.LoadUint64(&c.allowed), atomic.LoadUint64(&c.rejected)
}

// Cache는 내부 캐시를 반환합니다.
func (c *RateLimitedCache) Cache() *Cache {
	return c.cache
}

// =============================================================================
// Errors
// =============================================================================

// ErrRateLimited는 요청이 제한되었을 때 반환됩니다.
var ErrRateLimited = &RateLimitError{}

// RateLimitError는 Rate Limit 에러입니다.
type RateLimitError struct{}

func (e *RateLimitError) Error() string {
	return "rate limited"
}
