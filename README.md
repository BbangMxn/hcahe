# HCache - Hierarchical Cache Library

고성능 계층적 캐싱 라이브러리 (Go)

## 목표

1. **속도 우선**: Hot 모드 기본, L1은 항상 동기, 나머지는 비동기
2. **다양한 백엔드**: Memory, Redis, SQLite, S3 등 플러그인 방식
3. **자동 티어링**: 접근 패턴에 따라 자동 승격/강등
4. **제로 카피**: 불필요한 메모리 복사 최소화

## 아키텍처

```
┌─────────────────────────────────────────┐
│           HCache (Core Engine)          │
├─────────────────────────────────────────┤
│  L1: Memory (LRU)      │ ~1μs          │
│  L2: Redis/Memcached   │ ~1ms          │
│  L3: SQLite/RocksDB    │ ~5ms          │
│  L4: S3/GCS            │ ~100ms        │
└─────────────────────────────────────────┘
```

## 사용법

```go
import "github.com/bridgify/hcache"

// 캐시 생성
cache := hcache.New(
    hcache.WithLayer(memory.New(1000)),      // L1: 1000개 항목
    hcache.WithLayer(redis.New(redisOpts)),  // L2: Redis
    hcache.WithLayer(sqlite.New("cache.db")), // L3: SQLite
)

// 저장 (Hot 모드 - 기본)
cache.Set(ctx, "key", value)

// 저장 (Cold 모드 - 모든 계층 동기화)
cache.Set(ctx, "key", value, hcache.WithMode(hcache.Cold))

// 조회
value, layer, err := cache.Get(ctx, "key")

// 삭제
cache.Delete(ctx, "key")
```

## 프로젝트 구조

```
hcache/
├── core/           # 핵심 엔진
│   ├── cache.go    # 메인 캐시 로직
│   ├── layer.go    # 계층 관리
│   ├── options.go  # 옵션 설정
│   └── entry.go    # 캐시 엔트리
├── adapters/       # 저장소 어댑터
│   ├── memory/     # 인메모리 (LRU)
│   ├── redis/      # Redis/Valkey
│   ├── sqlite/     # SQLite
│   └── s3/         # AWS S3
├── serializer/     # 직렬화
├── compression/    # 압축
├── buffer/         # Write Buffer
├── tiering/        # 자동 티어링
├── metrics/        # 메트릭 수집
├── plugin/         # 플러그인 시스템
└── distributed/    # 분산 환경 지원
```

## 라이선스

MIT License
