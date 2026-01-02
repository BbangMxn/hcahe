package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/BbangMxn/hcahe/adapters/memory"
	"github.com/BbangMxn/hcahe/core"
)

// ============================================================================
// 벤치마크: Memory 어댑터 성능 측정
// ============================================================================

// BenchmarkMemorySet - Memory 어댑터 Set 성능
func BenchmarkMemorySet(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		entry := core.NewEntry(key, []byte("benchmark_value_data"), time.Hour)
		adapter.Set(ctx, entry)
	}
}

// BenchmarkMemoryGet - Memory 어댑터 Get 성능
func BenchmarkMemoryGet(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()

	// 테스트 데이터 준비
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		entry := core.NewEntry(key, []byte("benchmark_value_data"), time.Hour)
		adapter.Set(ctx, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i%10000)
		adapter.Get(ctx, key)
	}
}

// BenchmarkMemoryDelete - Memory 어댑터 Delete 성능
func BenchmarkMemoryDelete(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := fmt.Sprintf("bench_key_%d", i)
		entry := core.NewEntry(key, []byte("benchmark_value_data"), time.Hour)
		adapter.Set(ctx, entry)
		b.StartTimer()

		adapter.Delete(ctx, key)
	}
}

// BenchmarkMemorySetParallel - Memory 어댑터 병렬 Set 성능
func BenchmarkMemorySetParallel(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 1000000})
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("parallel_key_%d", i)
			entry := core.NewEntry(key, []byte("parallel_benchmark_value"), time.Hour)
			adapter.Set(ctx, entry)
			i++
		}
	})
}

// BenchmarkMemoryGetParallel - Memory 어댑터 병렬 Get 성능
func BenchmarkMemoryGetParallel(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()

	// 테스트 데이터 준비
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		entry := core.NewEntry(key, []byte("benchmark_value_data"), time.Hour)
		adapter.Set(ctx, entry)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bench_key_%d", i%10000)
			adapter.Get(ctx, key)
			i++
		}
	})
}

// ============================================================================
// 벤치마크: LRU 캐시 성능 측정
// ============================================================================

// BenchmarkLRUEviction - LRU 교체 성능
func BenchmarkLRUEviction(b *testing.B) {
	// 작은 캐시로 교체가 자주 발생하도록 설정
	adapter := memory.New(&memory.Config{MaxSize: 100})
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("evict_key_%d", i)
		entry := core.NewEntry(key, []byte("eviction_test_value"), time.Hour)
		adapter.Set(ctx, entry)
	}
}

// ============================================================================
// 벤치마크: 멀티 레이어 캐시 성능 측정
// ============================================================================

// BenchmarkMultiLayerCacheSet - 2계층 캐시 Set 성능
func BenchmarkMultiLayerCacheSet(b *testing.B) {
	l1 := memory.New(&memory.Config{MaxSize: 10000})
	l2 := memory.New(&memory.Config{MaxSize: 100000})

	cache := core.New(
		core.WithDefaultTTL(time.Hour),
		core.WithDefaultWriteMode(core.Hot),
	)
	cache.AddLayer(l1, core.DefaultLayerOptions("L1-Memory", 0))
	cache.AddLayer(l2, core.DefaultLayerOptions("L2-Memory", 1))

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("ml_key_%d", i)
		cache.Set(ctx, key, []byte("multi_layer_value"))
	}
}

// BenchmarkMultiLayerCacheGetL1Hit - 2계층 캐시 Get 성능 (L1 히트)
func BenchmarkMultiLayerCacheGetL1Hit(b *testing.B) {
	l1 := memory.New(&memory.Config{MaxSize: 100000})
	l2 := memory.New(&memory.Config{MaxSize: 100000})

	cache := core.New(
		core.WithDefaultTTL(time.Hour),
	)
	cache.AddLayer(l1, core.DefaultLayerOptions("L1-Memory", 0))
	cache.AddLayer(l2, core.DefaultLayerOptions("L2-Memory", 1))

	ctx := context.Background()

	// 테스트 데이터 준비
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("ml_key_%d", i)
		cache.Set(ctx, key, []byte("multi_layer_value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("ml_key_%d", i%10000)
		var result []byte
		cache.Get(ctx, key, &result)
	}
}

// BenchmarkMultiLayerCacheGetL2Hit - 2계층 캐시 Get 성능 (L2 히트, L1 미스)
func BenchmarkMultiLayerCacheGetL2Hit(b *testing.B) {
	l1 := memory.New(&memory.Config{MaxSize: 100}) // 작은 L1
	l2 := memory.New(&memory.Config{MaxSize: 100000})

	cache := core.New(
		core.WithDefaultTTL(time.Hour),
		core.WithDefaultWriteMode(core.Cold), // L2에도 저장
	)
	cache.AddLayer(l1, core.DefaultLayerOptions("L1-Memory", 0))
	cache.AddLayer(l2, core.DefaultLayerOptions("L2-Memory", 1))

	ctx := context.Background()

	// L2에 데이터 저장
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("ml_key_%d", i)
		cache.Set(ctx, key, []byte("multi_layer_value"))
	}

	// L1 비우기
	l1.Clear(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("ml_key_%d", i%10000)
		var result []byte
		cache.Get(ctx, key, &result)
	}
}

// ============================================================================
// 벤치마크: 데이터 크기별 성능
// ============================================================================

// BenchmarkMemorySetSmallValue - 작은 값 (32 bytes)
func BenchmarkMemorySetSmallValue(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()
	value := make([]byte, 32)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("small_%d", i)
		entry := core.NewEntry(key, value, time.Hour)
		adapter.Set(ctx, entry)
	}
}

// BenchmarkMemorySetMediumValue - 중간 값 (1 KB)
func BenchmarkMemorySetMediumValue(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()
	value := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("medium_%d", i)
		entry := core.NewEntry(key, value, time.Hour)
		adapter.Set(ctx, entry)
	}
}

// BenchmarkMemorySetLargeValue - 큰 값 (64 KB)
func BenchmarkMemorySetLargeValue(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 10000})
	ctx := context.Background()
	value := make([]byte, 64*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("large_%d", i)
		entry := core.NewEntry(key, value, time.Hour)
		adapter.Set(ctx, entry)
	}
}

// BenchmarkMemorySetHugeValue - 매우 큰 값 (1 MB)
func BenchmarkMemorySetHugeValue(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 1000})
	ctx := context.Background()
	value := make([]byte, 1024*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("huge_%d", i)
		entry := core.NewEntry(key, value, time.Hour)
		adapter.Set(ctx, entry)
	}
}

// ============================================================================
// 벤치마크: 혼합 워크로드
// ============================================================================

// BenchmarkMixedWorkload - 읽기 80%, 쓰기 20% 혼합
func BenchmarkMixedWorkload(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()

	// 초기 데이터 준비
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("mixed_%d", i)
		entry := core.NewEntry(key, []byte("mixed_workload_value"), time.Hour)
		adapter.Set(ctx, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20% 쓰기
			key := fmt.Sprintf("mixed_new_%d", i)
			entry := core.NewEntry(key, []byte("new_value"), time.Hour)
			adapter.Set(ctx, entry)
		} else {
			// 80% 읽기
			key := fmt.Sprintf("mixed_%d", i%10000)
			adapter.Get(ctx, key)
		}
	}
}

// BenchmarkMixedWorkloadParallel - 병렬 혼합 워크로드
func BenchmarkMixedWorkloadParallel(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()

	// 초기 데이터 준비
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("mixed_%d", i)
		entry := core.NewEntry(key, []byte("mixed_workload_value"), time.Hour)
		adapter.Set(ctx, entry)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				key := fmt.Sprintf("mixed_new_%d", i)
				entry := core.NewEntry(key, []byte("new_value"), time.Hour)
				adapter.Set(ctx, entry)
			} else {
				key := fmt.Sprintf("mixed_%d", i%10000)
				adapter.Get(ctx, key)
			}
			i++
		}
	})
}

// ============================================================================
// 벤치마크: 배치 연산
// ============================================================================

// BenchmarkMemorySetMany - 배치 Set 성능
func BenchmarkMemorySetMany(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()

	// 배치 크기 100
	entries := make([]*core.Entry, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("batch_key_%d", i)
		entries[i] = core.NewEntry(key, []byte("batch_value"), time.Hour)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.SetMany(ctx, entries)
	}
}

// BenchmarkMemoryGetMany - 배치 Get 성능
func BenchmarkMemoryGetMany(b *testing.B) {
	adapter := memory.New(&memory.Config{MaxSize: 100000})
	ctx := context.Background()

	// 테스트 데이터 준비
	keys := make([]string, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("batch_key_%d", i)
		keys[i] = key
		entry := core.NewEntry(key, []byte("batch_value"), time.Hour)
		adapter.Set(ctx, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.GetMany(ctx, keys)
	}
}
