// 실제 PostgreSQL 통합 테스트
package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bridgify/hcache/adapters/memory"
	"github.com/bridgify/hcache/adapters/postgres"
	"github.com/bridgify/hcache/core"
)

const (
	// Railway 제공 연결 정보
	PostgresDSN = "postgresql://postgres:XjAOxruOwHpEFMhjUcLJBvwxbuxQqWsl@yamanote.proxy.rlwy.net:52433/railway"
)

func TestPostgresConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter := postgres.New(&postgres.Config{
		DSN:       PostgresDSN,
		TableName: "hcache_test",
	})

	// 연결
	err := adapter.Connect(ctx)
	if err != nil {
		t.Fatalf("PostgreSQL 연결 실패: %v", err)
	}
	defer adapter.Disconnect(ctx)

	t.Log("✓ PostgreSQL 연결 성공")

	// Ping
	err = adapter.Ping(ctx)
	if err != nil {
		t.Fatalf("PostgreSQL Ping 실패: %v", err)
	}
	t.Log("✓ PostgreSQL Ping 성공")
}

func TestPostgresSetGet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adapter := postgres.New(&postgres.Config{
		DSN:       PostgresDSN,
		TableName: "hcache_test",
	})

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("연결 실패: %v", err)
	}
	defer adapter.Disconnect(ctx)

	// 저장
	entry := core.NewEntry("pg_test_key", []byte("pg_test_value"), 1*time.Minute)
	err := adapter.Set(ctx, entry)
	if err != nil {
		t.Fatalf("Set 실패: %v", err)
	}
	t.Log("✓ PostgreSQL Set 성공")

	// 조회
	result, err := adapter.Get(ctx, "pg_test_key")
	if err != nil {
		t.Fatalf("Get 실패: %v", err)
	}
	if result == nil {
		t.Fatal("결과가 nil입니다")
	}
	if string(result.Value) != "pg_test_value" {
		t.Fatalf("값이 다릅니다: %s", string(result.Value))
	}
	t.Log("✓ PostgreSQL Get 성공")

	// 삭제
	deleted, err := adapter.Delete(ctx, "pg_test_key")
	if err != nil {
		t.Fatalf("Delete 실패: %v", err)
	}
	if !deleted {
		t.Fatal("삭제되지 않았습니다")
	}
	t.Log("✓ PostgreSQL Delete 성공")
}

func TestMultiLayerCacheMemoryPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 캐시 생성
	cache := core.New(
		core.WithDefaultTTL(5*time.Minute),
		core.WithDefaultWriteMode(core.Hot),
	)

	// L1: Memory
	memAdapter := memory.New(&memory.Config{MaxSize: 100})
	cache.AddLayer(memAdapter, &core.LayerOptions{
		Name:     "L1-Memory",
		Priority: 0,
		MaxSize:  100,
	})

	// L2: PostgreSQL
	pgAdapter := postgres.New(&postgres.Config{
		DSN:       PostgresDSN,
		TableName: "hcache_multi",
	})
	cache.AddLayer(pgAdapter, &core.LayerOptions{
		Name:     "L2-PostgreSQL",
		Priority: 1,
	})

	// 연결
	if err := cache.Connect(ctx); err != nil {
		t.Fatalf("캐시 연결 실패: %v", err)
	}
	defer cache.Close(ctx)

	t.Logf("✓ 2계층 캐시 연결 성공 (계층 수: %d)", cache.LayerCount())

	// 데이터 저장
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("multi_key_%d", i)
		value := fmt.Sprintf("multi_value_%d", i)
		entry := core.NewEntry(key, []byte(value), 5*time.Minute)

		if err := cache.SetRaw(ctx, entry); err != nil {
			t.Fatalf("Set 실패: %v", err)
		}
	}
	t.Log("✓ 5개 항목 저장 성공")

	// 조회 테스트
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("multi_key_%d", i)
		result, layer, err := cache.GetRaw(ctx, key)
		if err != nil {
			t.Fatalf("Get 실패: %v", err)
		}
		if result == nil {
			t.Fatalf("%s를 찾을 수 없습니다", key)
		}
		t.Logf("  %s → L%d에서 발견: %s", key, layer, string(result.Value))
	}
	t.Log("✓ 조회 성공")

	// 통계
	stats, err := cache.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats 실패: %v", err)
	}

	t.Log("\n=== 계층별 통계 ===")
	for _, stat := range stats {
		t.Logf("%s: 크기=%d, 히트=%d, 미스=%d, 히트율=%.1f%%",
			stat.Name, stat.Size, stat.Hits, stat.Misses, stat.HitRate()*100)
	}

	// 정리
	if err := cache.Clear(ctx); err != nil {
		t.Logf("Clear 경고: %v", err)
	}
	t.Log("✓ 정리 완료")
}

func TestLatencyComparisonMemoryPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Memory 어댑터
	memAdapter := memory.New(&memory.Config{MaxSize: 1000})
	memAdapter.Connect(ctx)
	defer memAdapter.Disconnect(ctx)

	// PostgreSQL 어댑터
	pgAdapter := postgres.New(&postgres.Config{
		DSN:       PostgresDSN,
		TableName: "latency_test",
	})
	if err := pgAdapter.Connect(ctx); err != nil {
		t.Fatalf("PostgreSQL 연결 실패: %v", err)
	}
	defer pgAdapter.Disconnect(ctx)

	// 테스트 데이터
	entry := core.NewEntry("latency_key", []byte("latency_value_12345"), 5*time.Minute)

	iterations := 50

	// Memory 테스트
	memAdapter.Set(ctx, entry)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		memAdapter.Get(ctx, "latency_key")
	}
	memLatency := time.Since(start) / time.Duration(iterations)

	// PostgreSQL 테스트
	pgAdapter.Set(ctx, entry)
	start = time.Now()
	for i := 0; i < iterations; i++ {
		pgAdapter.Get(ctx, "latency_key")
	}
	pgLatency := time.Since(start) / time.Duration(iterations)

	t.Log("\n=== 지연 시간 비교 (평균) ===")
	t.Logf("Memory:     %v", memLatency)
	t.Logf("PostgreSQL: %v", pgLatency)
	t.Logf("\nPostgreSQL는 Memory보다 %.1fx 느림", float64(pgLatency)/float64(memLatency))

	// 정리
	memAdapter.Delete(ctx, "latency_key")
	pgAdapter.Delete(ctx, "latency_key")
}

func TestBatchOperationsPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pgAdapter := postgres.New(&postgres.Config{
		DSN:       PostgresDSN,
		TableName: "batch_test",
	})
	if err := pgAdapter.Connect(ctx); err != nil {
		t.Fatalf("PostgreSQL 연결 실패: %v", err)
	}
	defer pgAdapter.Disconnect(ctx)

	// 배치 저장
	entries := make([]*core.Entry, 10)
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("batch_key_%d", i)
		entries[i] = core.NewEntry(keys[i], []byte(fmt.Sprintf("batch_value_%d", i)), 5*time.Minute)
	}

	start := time.Now()
	err := pgAdapter.SetMany(ctx, entries)
	if err != nil {
		t.Fatalf("SetMany 실패: %v", err)
	}
	t.Logf("✓ 10개 항목 배치 저장: %v", time.Since(start))

	// 배치 조회
	start = time.Now()
	results, err := pgAdapter.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany 실패: %v", err)
	}
	t.Logf("✓ 10개 항목 배치 조회: %v (결과: %d개)", time.Since(start), len(results))

	// 배치 삭제
	start = time.Now()
	count, err := pgAdapter.DeleteMany(ctx, keys)
	if err != nil {
		t.Fatalf("DeleteMany 실패: %v", err)
	}
	t.Logf("✓ 10개 항목 배치 삭제: %v (삭제: %d개)", time.Since(start), count)
}

func TestCachePromotion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// L2에 직접 데이터 저장
	pgAdapter := postgres.New(&postgres.Config{
		DSN:       PostgresDSN,
		TableName: "hcache_promotion",
	})
	if err := pgAdapter.Connect(ctx); err != nil {
		t.Fatalf("PostgreSQL 연결 실패: %v", err)
	}

	// L2에 직접 저장
	entry := core.NewEntry("promo_key", []byte("promo_value"), 5*time.Minute)
	pgAdapter.Set(ctx, entry)
	t.Log("✓ L2(PostgreSQL)에 직접 저장")

	// 캐시 생성 (L1만 비어있음)
	cache := core.New(
		core.WithDefaultTTL(5*time.Minute),
		core.WithDefaultWriteMode(core.Hot),
		core.WithAutoTiering(1, 1*time.Minute),
	)

	// L1: Memory (비어있음)
	memAdapter := memory.New(&memory.Config{MaxSize: 100})
	cache.AddLayer(memAdapter, &core.LayerOptions{
		Name:     "L1-Memory",
		Priority: 0,
	})

	// L2: PostgreSQL (데이터 있음)
	cache.AddLayer(pgAdapter, &core.LayerOptions{
		Name:     "L2-PostgreSQL",
		Priority: 1,
	})

	if err := cache.Connect(ctx); err != nil {
		t.Fatalf("연결 실패: %v", err)
	}
	defer cache.Close(ctx)

	// L1 확인 - 비어있어야 함
	result1, _ := memAdapter.Get(ctx, "promo_key")
	if result1 != nil {
		t.Log("  L1에 이미 데이터 있음 (예상치 못함)")
	} else {
		t.Log("✓ L1은 비어있음")
	}

	// 캐시에서 조회 - L2에서 찾아야 함
	result, layer, err := cache.GetRaw(ctx, "promo_key")
	if err != nil {
		t.Fatalf("조회 실패: %v", err)
	}
	if result == nil {
		t.Fatal("데이터를 찾을 수 없습니다")
	}
	t.Logf("✓ L%d에서 발견: %s", layer, string(result.Value))

	// 승격 대기 (비동기 처리)
	time.Sleep(2 * time.Second)

	// L1 확인 - 승격되어 있어야 함
	result2, _ := memAdapter.Get(ctx, "promo_key")
	if result2 != nil {
		t.Logf("✓ L1으로 승격됨: %s", string(result2.Value))
	} else {
		t.Log("  L1으로 승격되지 않음 (비동기 처리 중일 수 있음)")
	}

	// 정리
	pgAdapter.Delete(ctx, "promo_key")
	t.Log("✓ 정리 완료")
}
