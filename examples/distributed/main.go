// 분산 캐시 예제 - Redis를 사용한 다중 인스턴스 캐시
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/bridgify/hcache"
)

// 환경 변수에서 설정을 가져옵니다.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	ctx := context.Background()

	// 환경 변수에서 연결 정보 로드
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	pgConnStr := getEnv("POSTGRES_URL", "")

	// ==========================================================================
	// 1. Redis만 사용 (메모리 없이)
	// ==========================================================================
	fmt.Println("=== 1. Redis Only ===")

	// 메모리 없이 Redis만 사용하면 여러 인스턴스가 같은 캐시를 공유
	cache, err := hcache.WithRedis(redisAddr, redisPassword, 0)
	if err != nil {
		fmt.Printf("Redis connection failed: %v\n", err)
		fmt.Println("Skipping Redis examples...")
	} else {
		cache.Set(ctx, "shared:key", []byte("This is shared across instances"))

		var data []byte
		if layer, _ := cache.Get(ctx, "shared:key", &data); layer >= 0 {
			fmt.Printf("Shared data: %s\n", string(data))
		}
	}

	// ==========================================================================
	// 2. Memory + Redis 2계층
	// ==========================================================================
	fmt.Println("\n=== 2. Memory + Redis (2-tier) ===")

	cache2, err := hcache.WithMemoryAndRedis(10000, redisAddr, redisPassword, 0)
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
	} else {
		// 저장 - L1(Memory)에 빠르게 저장, L2(Redis)에 비동기 전파
		cache2.Set(ctx, "user:session:abc", []byte("session_data"))

		var session []byte
		layer, _ := cache2.Get(ctx, "user:session:abc", &session)
		fmt.Printf("Found in L%d: %s\n", layer, string(session))
	}

	// ==========================================================================
	// 3. Redis + PostgreSQL (메모리 없이)
	// ==========================================================================
	fmt.Println("\n=== 3. Redis + PostgreSQL (No Memory) ===")

	// 이 구성은 메모리를 사용하지 않으므로:
	// - 컨테이너 재시작해도 캐시 유지
	// - 여러 Pod 간 캐시 공유
	// - Redis가 L1 역할 (빠른 읽기)
	// - PostgreSQL이 L2 역할 (영구 저장)

	if pgConnStr == "" {
		fmt.Println("POSTGRES_URL not set, skipping PostgreSQL examples...")
	} else {
		cache3, err := hcache.WithRedisAndPostgres(redisAddr, redisPassword, 0, pgConnStr)
		if err != nil {
			fmt.Printf("Connection failed: %v\n", err)
		} else {
			cache3.Set(ctx, "config:app", []byte(`{"version":"1.0"}`))
			fmt.Println("Stored in Redis + PostgreSQL")
		}
	}

	// ==========================================================================
	// 4. 3계층 구성
	// ==========================================================================
	fmt.Println("\n=== 4. 3-tier: Memory + Redis + PostgreSQL ===")

	if pgConnStr == "" {
		fmt.Println("POSTGRES_URL not set, skipping 3-tier example...")
	} else {
		cache4, err := hcache.WithThreeLayers(
			10000,         // Memory size
			redisAddr,     // Redis
			redisPassword, // Redis password
			0,             // Redis db
			pgConnStr,     // PostgreSQL
		)
		if err != nil {
			fmt.Printf("Connection failed: %v\n", err)
		} else {
			// 저장: Memory(30s) → Redis(10m) → PostgreSQL(24h)
			cache4.Set(ctx, "hot:data", []byte("frequently accessed"))

			stats, _ := cache4.Stats(ctx)
			fmt.Println("3-tier cache configured:")
			for _, s := range stats {
				fmt.Printf("  - %s: TTL varies by tier\n", s.Name)
			}
		}
	}

	// ==========================================================================
	// 5. 빌더로 커스텀 구성
	// ==========================================================================
	fmt.Println("\n=== 5. Custom Configuration ===")

	builder := hcache.NewBuilder().
		WithDefaultTTL(30*time.Minute).
		WithWriteMode(hcache.Warm). // L1, L2까지 동기 저장
		AddMemoryLayer(5000, 30*time.Second).
		AddRedisLayer(redisAddr, redisPassword, 0, 5*time.Minute).
		EnablePrediction(3). // 3번 접근 시 프리페치
		EnableAdaptiveTTL(time.Minute, time.Hour).
		EnableTags()

	if pgConnStr != "" {
		builder = builder.AddPostgresLayer(pgConnStr, 1*time.Hour)
	}

	cache5, err := builder.BuildBasic()
	if err != nil {
		fmt.Printf("Build failed: %v\n", err)
	} else {
		fmt.Println("Custom cache with intelligent features ready!")
		_ = cache5
	}

	fmt.Println("\n✓ Distributed cache examples completed!")
}
