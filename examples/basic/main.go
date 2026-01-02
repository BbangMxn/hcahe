// 기본 사용 예제
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bridgify/hcache"
)

func main() {
	ctx := context.Background()

	// ==========================================================================
	// 1. 가장 간단한 사용법
	// ==========================================================================
	fmt.Println("=== 1. Quick Start ===")

	cache := hcache.Quick()

	// 저장
	cache.Set(ctx, "user:1", []byte(`{"name":"Kim","age":25}`))

	// 조회
	var data []byte
	if layer, _ := cache.Get(ctx, "user:1", &data); layer >= 0 {
		fmt.Printf("Found: %s\n", string(data))
	}

	// ==========================================================================
	// 2. 메모리 크기 지정
	// ==========================================================================
	fmt.Println("\n=== 2. Memory with Size ===")

	cache2 := hcache.WithMemory(50000, 10*time.Minute)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("item:%d", i)
		cache2.Set(ctx, key, []byte(fmt.Sprintf("value_%d", i)))
	}

	stats, _ := cache2.Stats(ctx)
	fmt.Printf("Cache size: %d items\n", stats[0].Size)

	// ==========================================================================
	// 3. 빌더 패턴으로 세밀한 설정
	// ==========================================================================
	fmt.Println("\n=== 3. Builder Pattern ===")

	cache3, err := hcache.NewBuilder().
		WithDefaultTTL(5*time.Minute).
		WithWriteMode(hcache.Hot).
		AddMemoryLayer(10000, time.Minute).
		WithJSONSerializer().
		WithGzipCompression().
		EnableTags().
		Build()

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// 구조체 저장 (자동 직렬화)
	type User struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	cache3.Set(ctx, "user:100", &User{ID: 100, Name: "Park"})

	var user User
	if layer, _ := cache3.Get(ctx, "user:100", &user); layer >= 0 {
		fmt.Printf("User: %+v\n", user)
	}

	// ==========================================================================
	// 4. 지능형 캐시
	// ==========================================================================
	fmt.Println("\n=== 4. Intelligent Cache ===")

	icache := hcache.Intelligent(
		hcache.WithPrediction(),                     // 예측 캐싱
		hcache.WithSmartTTL(time.Minute, time.Hour), // 적응형 TTL
		hcache.WithTags(),                           // 태그 지원
		hcache.WithDependencies(),                   // 의존성 추적
	)

	// 태그와 함께 저장
	icache.SetWithTags(ctx, "product:1", []byte("Product A"), []string{"products", "category:electronics"})
	icache.SetWithTags(ctx, "product:2", []byte("Product B"), []string{"products", "category:electronics"})
	icache.SetWithTags(ctx, "product:3", []byte("Product C"), []string{"products", "category:books"})

	// 태그로 일괄 무효화
	fmt.Println("Invalidating all 'category:electronics' items...")
	icache.InvalidateByTag(ctx, "category:electronics")

	// product:3만 남아있음
	var p3 []byte
	if layer, _ := icache.Get(ctx, "product:3", &p3); layer >= 0 {
		fmt.Printf("Product 3 still exists: %s\n", string(p3))
	}

	// 의존성과 함께 저장
	icache.SetWithDependencies(ctx, "order:1", []byte("Order 1"), []string{"user:1", "product:3"})

	// user:1이 변경되면 order:1도 무효화
	fmt.Println("Invalidating user:1 with dependents...")
	icache.InvalidateWithDependents(ctx, "user:1")

	fmt.Println("\n✓ All examples completed!")
}
