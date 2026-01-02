// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 캐시 워밍(Pre-warming)을 구현합니다.
package core

import (
	"context"
	"sync"
	"time"
)

// =============================================================================
// Cache Warming: 캐시 예열
// =============================================================================
// 시작 시 자주 사용되는 데이터를 미리 로드하여
// Cold Start 문제를 해결합니다.
// =============================================================================

// WarmingSource는 워밍 데이터 소스 인터페이스입니다.
type WarmingSource interface {
	// GetKeys는 워밍할 키 목록을 반환합니다.
	GetKeys(ctx context.Context) ([]string, error)

	// GetData는 키에 해당하는 데이터를 반환합니다.
	GetData(ctx context.Context, key string) ([]byte, time.Duration, error)
}

// WarmingConfig는 워밍 설정입니다.
type WarmingConfig struct {
	// Concurrency는 동시 워밍 작업 수입니다.
	Concurrency int

	// BatchSize는 한 번에 처리할 키 수입니다.
	BatchSize int

	// Timeout은 전체 워밍 타임아웃입니다.
	Timeout time.Duration

	// OnProgress는 진행 상황 콜백입니다.
	OnProgress func(loaded, total int)

	// OnError는 에러 발생 시 콜백입니다.
	OnError func(key string, err error)

	// ContinueOnError는 에러 발생 시 계속 진행할지 여부입니다.
	ContinueOnError bool
}

// DefaultWarmingConfig는 기본 워밍 설정을 반환합니다.
func DefaultWarmingConfig() *WarmingConfig {
	return &WarmingConfig{
		Concurrency:     10,
		BatchSize:       100,
		Timeout:         5 * time.Minute,
		ContinueOnError: true,
	}
}

// CacheWarmer는 캐시 워밍을 수행합니다.
type CacheWarmer struct {
	cache  *Cache
	config *WarmingConfig
}

// NewCacheWarmer는 새로운 캐시 워머를 생성합니다.
func NewCacheWarmer(cache *Cache, config *WarmingConfig) *CacheWarmer {
	if config == nil {
		config = DefaultWarmingConfig()
	}

	return &CacheWarmer{
		cache:  cache,
		config: config,
	}
}

// WarmFromSource는 소스에서 데이터를 로드하여 캐시를 워밍합니다.
func (w *CacheWarmer) WarmFromSource(ctx context.Context, source WarmingSource) (*WarmingResult, error) {
	ctx, cancel := context.WithTimeout(ctx, w.config.Timeout)
	defer cancel()

	// 키 목록 가져오기
	keys, err := source.GetKeys(ctx)
	if err != nil {
		return nil, err
	}

	result := &WarmingResult{
		Total:     len(keys),
		StartTime: time.Now(),
	}

	// 워커 풀
	keyChan := make(chan string, w.config.BatchSize)
	var wg sync.WaitGroup
	var mu sync.Mutex

	// 워커 시작
	for i := 0; i < w.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for key := range keyChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				data, ttl, err := source.GetData(ctx, key)
				if err != nil {
					mu.Lock()
					result.Errors++
					mu.Unlock()

					if w.config.OnError != nil {
						w.config.OnError(key, err)
					}

					if !w.config.ContinueOnError {
						return
					}
					continue
				}

				// 캐시에 저장
				entry := NewEntry(key, data, ttl)
				if err := w.cache.SetRaw(ctx, entry); err != nil {
					mu.Lock()
					result.Errors++
					mu.Unlock()
					continue
				}

				mu.Lock()
				result.Loaded++
				if w.config.OnProgress != nil {
					w.config.OnProgress(result.Loaded, result.Total)
				}
				mu.Unlock()
			}
		}()
	}

	// 키 전송
	for _, key := range keys {
		select {
		case <-ctx.Done():
			break
		case keyChan <- key:
		}
	}
	close(keyChan)

	wg.Wait()

	result.Duration = time.Since(result.StartTime)
	return result, nil
}

// WarmFromMap은 맵에서 데이터를 로드하여 캐시를 워밍합니다.
func (w *CacheWarmer) WarmFromMap(ctx context.Context, data map[string][]byte, ttl time.Duration) (*WarmingResult, error) {
	result := &WarmingResult{
		Total:     len(data),
		StartTime: time.Now(),
	}

	entries := make([]*Entry, 0, len(data))
	for key, value := range data {
		entries = append(entries, NewEntry(key, value, ttl))
	}

	// 배치로 저장
	for i := 0; i < len(entries); i += w.config.BatchSize {
		end := i + w.config.BatchSize
		if end > len(entries) {
			end = len(entries)
		}

		batch := entries[i:end]

		// 각 계층에 저장
		for _, layer := range w.cache.Layers() {
			if err := layer.SetMany(ctx, batch); err != nil {
				result.Errors++
				if !w.config.ContinueOnError {
					result.Duration = time.Since(result.StartTime)
					return result, err
				}
			}
		}

		result.Loaded += len(batch)

		if w.config.OnProgress != nil {
			w.config.OnProgress(result.Loaded, result.Total)
		}
	}

	result.Duration = time.Since(result.StartTime)
	return result, nil
}

// WarmFromKeys는 하위 계층에서 상위 계층으로 데이터를 로드합니다.
func (w *CacheWarmer) WarmFromKeys(ctx context.Context, keys []string) (*WarmingResult, error) {
	result := &WarmingResult{
		Total:     len(keys),
		StartTime: time.Now(),
	}

	for _, key := range keys {
		select {
		case <-ctx.Done():
			break
		default:
		}

		// Get을 호출하면 자동으로 상위 계층으로 승격됨
		var dest interface{}
		layer, _ := w.cache.Get(ctx, key, &dest)

		if layer >= 0 {
			result.Loaded++
		} else {
			result.Errors++
		}

		if w.config.OnProgress != nil {
			w.config.OnProgress(result.Loaded, result.Total)
		}
	}

	result.Duration = time.Since(result.StartTime)
	return result, nil
}

// WarmingResult는 워밍 결과입니다.
type WarmingResult struct {
	Total     int
	Loaded    int
	Errors    int
	Duration  time.Duration
	StartTime time.Time
}

// SuccessRate는 성공률을 반환합니다.
func (r *WarmingResult) SuccessRate() float64 {
	if r.Total == 0 {
		return 0
	}
	return float64(r.Loaded) / float64(r.Total)
}

// =============================================================================
// Static Warming Source: 정적 데이터 소스
// =============================================================================

// StaticWarmingSource는 정적 데이터를 제공하는 소스입니다.
type StaticWarmingSource struct {
	data map[string][]byte
	ttl  time.Duration
}

// NewStaticWarmingSource는 새로운 정적 워밍 소스를 생성합니다.
func NewStaticWarmingSource(data map[string][]byte, ttl time.Duration) *StaticWarmingSource {
	return &StaticWarmingSource{
		data: data,
		ttl:  ttl,
	}
}

func (s *StaticWarmingSource) GetKeys(ctx context.Context) ([]string, error) {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (s *StaticWarmingSource) GetData(ctx context.Context, key string) ([]byte, time.Duration, error) {
	data, ok := s.data[key]
	if !ok {
		return nil, 0, nil
	}
	return data, s.ttl, nil
}

// =============================================================================
// Database Warming Source: DB에서 워밍
// =============================================================================

// DBWarmingSource는 데이터베이스에서 데이터를 로드하는 소스입니다.
type DBWarmingSource struct {
	// QueryKeys는 키 목록을 가져오는 함수입니다.
	QueryKeys func(ctx context.Context) ([]string, error)

	// QueryData는 키에 해당하는 데이터를 가져오는 함수입니다.
	QueryData func(ctx context.Context, key string) ([]byte, error)

	// TTL은 기본 TTL입니다.
	TTL time.Duration
}

func (s *DBWarmingSource) GetKeys(ctx context.Context) ([]string, error) {
	return s.QueryKeys(ctx)
}

func (s *DBWarmingSource) GetData(ctx context.Context, key string) ([]byte, time.Duration, error) {
	data, err := s.QueryData(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return data, s.TTL, nil
}
