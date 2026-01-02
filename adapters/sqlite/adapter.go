// Package sqlite는 SQLite 캐시 어댑터를 구현합니다.
// L3 캐시로 사용되며, 로컬 디스크에 영속적으로 저장됩니다.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bridgify/hcache/core"
	_ "github.com/mattn/go-sqlite3"
)

// =============================================================================
// SQLiteAdapter: SQLite 캐시 어댑터
// =============================================================================
// SQLiteAdapter는 SQLite 데이터베이스에 데이터를 저장합니다.
// 로컬 디스크 기반으로 프로세스 재시작 후에도 데이터가 유지됩니다.
//
// 특징:
// - ~5ms 접근 시간
// - 프로세스 재시작 후에도 데이터 유지
// - 단일 파일로 간편한 관리
// - SQL 기반 쿼리 가능
// =============================================================================

// Config는 SQLite 어댑터 설정입니다.
type Config struct {
	// Path는 데이터베이스 파일 경로입니다.
	// ":memory:"를 사용하면 인메모리 데이터베이스가 됩니다.
	Path string

	// MaxOpenConns는 최대 열린 연결 수입니다.
	MaxOpenConns int

	// MaxIdleConns는 최대 유휴 연결 수입니다.
	MaxIdleConns int

	// ConnMaxLifetime은 연결의 최대 수명입니다.
	ConnMaxLifetime time.Duration

	// CleanupInterval은 만료된 항목 정리 주기입니다.
	CleanupInterval time.Duration

	// WALMode는 WAL(Write-Ahead Logging) 모드 사용 여부입니다.
	// WAL 모드는 동시 읽기/쓰기 성능을 향상시킵니다.
	WALMode bool

	// CacheSize는 SQLite 페이지 캐시 크기입니다. (페이지 단위)
	// 음수면 KB 단위로 해석됩니다. (예: -64000 = 64MB)
	CacheSize int
}

// DefaultConfig는 기본 설정을 반환합니다.
func DefaultConfig() *Config {
	return &Config{
		Path:            "hcache.db",
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 1 * time.Hour,
		CleanupInterval: 5 * time.Minute,
		WALMode:         true,
		CacheSize:       -64000, // 64MB
	}
}

// Adapter는 SQLite 캐시 어댑터입니다.
type Adapter struct {
	// config는 어댑터 설정입니다.
	config *Config

	// db는 SQLite 데이터베이스 연결입니다.
	db *sql.DB

	// connected는 연결 상태입니다.
	connected bool

	// 메트릭
	hits       uint64
	misses     uint64
	evictions  uint64
	getLatency int64
	getCount   uint64
	setLatency int64
	setCount   uint64

	// cleanup 관련
	cleanupTicker *time.Ticker
	closeCh       chan struct{}

	// mu는 연결 상태 변경을 위한 뮤텍스입니다.
	mu sync.RWMutex
}

// =============================================================================
// Adapter 생성자
// =============================================================================

// New는 새로운 SQLite 어댑터를 생성합니다.
//
// Parameters:
//   - config: 어댑터 설정 (nil이면 기본값)
//
// Returns:
//   - *Adapter: 생성된 어댑터
func New(config *Config) *Adapter {
	if config == nil {
		config = DefaultConfig()
	}

	return &Adapter{
		config:  config,
		closeCh: make(chan struct{}),
	}
}

// =============================================================================
// Adapter 인터페이스 구현 - 기본 정보
// =============================================================================

// Name은 어댑터 이름을 반환합니다.
func (a *Adapter) Name() string {
	return "sqlite"
}

// Type은 어댑터 유형을 반환합니다.
func (a *Adapter) Type() core.AdapterType {
	return core.TypeDisk
}

// =============================================================================
// Adapter 인터페이스 구현 - 연결 관리
// =============================================================================

// Connect는 SQLite에 연결하고 테이블을 생성합니다.
func (a *Adapter) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.connected {
		return nil
	}

	// 디렉토리 생성
	if a.config.Path != ":memory:" {
		dir := filepath.Dir(a.config.Path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create directory failed: %w", err)
		}
	}

	// 데이터베이스 연결
	dsn := a.config.Path
	if a.config.Path != ":memory:" {
		dsn = fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000", a.config.Path)
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("sqlite open failed: %w", err)
	}

	// 연결 풀 설정
	db.SetMaxOpenConns(a.config.MaxOpenConns)
	db.SetMaxIdleConns(a.config.MaxIdleConns)
	db.SetConnMaxLifetime(a.config.ConnMaxLifetime)

	// 연결 테스트
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("sqlite ping failed: %w", err)
	}

	// PRAGMA 설정
	pragmas := []string{
		fmt.Sprintf("PRAGMA cache_size = %d", a.config.CacheSize),
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456", // 256MB
	}
	if a.config.WALMode {
		pragmas = append(pragmas, "PRAGMA journal_mode = WAL")
	}

	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			db.Close()
			return fmt.Errorf("pragma failed: %w", err)
		}
	}

	// 테이블 생성
	createSQL := `
		CREATE TABLE IF NOT EXISTS cache (
			key TEXT PRIMARY KEY,
			value BLOB NOT NULL,
			created_at INTEGER NOT NULL,
			expires_at INTEGER,
			access_count INTEGER DEFAULT 0,
			last_accessed_at INTEGER,
			size INTEGER,
			compressed INTEGER DEFAULT 0,
			compression_type TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_cache_expires_at ON cache(expires_at);
		CREATE INDEX IF NOT EXISTS idx_cache_last_accessed_at ON cache(last_accessed_at);
	`
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		db.Close()
		return fmt.Errorf("create table failed: %w", err)
	}

	a.db = db
	a.connected = true

	// 자동 정리 시작
	if a.config.CleanupInterval > 0 {
		a.cleanupTicker = time.NewTicker(a.config.CleanupInterval)
		go a.cleanupLoop()
	}

	return nil
}

// Disconnect는 SQLite 연결을 종료합니다.
func (a *Adapter) Disconnect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.connected || a.db == nil {
		return nil
	}

	// 정리 루프 종료
	if a.cleanupTicker != nil {
		a.cleanupTicker.Stop()
		close(a.closeCh)
	}

	err := a.db.Close()
	a.connected = false
	a.db = nil

	return err
}

// IsConnected는 연결 상태를 반환합니다.
func (a *Adapter) IsConnected() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.connected
}

// Ping은 SQLite가 응답하는지 확인합니다.
func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("not connected")
	}

	return db.PingContext(ctx)
}

// =============================================================================
// Adapter 인터페이스 구현 - CRUD
// =============================================================================

// Get은 키에 해당하는 엔트리를 조회합니다.
func (a *Adapter) Get(ctx context.Context, key string) (*core.Entry, error) {
	start := time.Now()
	defer func() {
		atomic.AddInt64(&a.getLatency, time.Since(start).Nanoseconds())
		atomic.AddUint64(&a.getCount, 1)
	}()

	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("not connected")
	}

	// 조회
	var value []byte
	var createdAt, expiresAt, lastAccessedAt sql.NullInt64
	var accessCount int64
	var size int
	var compressed int
	var compressionType sql.NullString

	query := `
		SELECT value, created_at, expires_at, access_count, last_accessed_at, size, compressed, compression_type
		FROM cache WHERE key = ?
	`
	err := db.QueryRowContext(ctx, query, key).Scan(
		&value, &createdAt, &expiresAt, &accessCount, &lastAccessedAt,
		&size, &compressed, &compressionType,
	)

	if err == sql.ErrNoRows {
		atomic.AddUint64(&a.misses, 1)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite query error: %w", err)
	}

	// Entry 생성
	entry := &core.Entry{
		Key:         key,
		Value:       value,
		CreatedAt:   time.Unix(createdAt.Int64, 0),
		AccessCount: uint64(accessCount),
		Size:        size,
		Compressed:  compressed == 1,
	}

	if expiresAt.Valid {
		entry.ExpiresAt = time.Unix(expiresAt.Int64, 0)
	}
	if lastAccessedAt.Valid {
		entry.LastAccessedAt = time.Unix(lastAccessedAt.Int64, 0)
	}
	if compressionType.Valid {
		entry.CompressionType = compressionType.String
	}

	// 접근 기록 업데이트 (비동기)
	go func() {
		updateCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		a.updateAccessStats(updateCtx, key)
	}()

	atomic.AddUint64(&a.hits, 1)
	return entry, nil
}

// updateAccessStats는 접근 통계를 업데이트합니다.
func (a *Adapter) updateAccessStats(ctx context.Context, key string) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return
	}

	now := time.Now().Unix()
	_, _ = db.ExecContext(ctx,
		"UPDATE cache SET access_count = access_count + 1, last_accessed_at = ? WHERE key = ?",
		now, key,
	)
}

// Set은 엔트리를 저장합니다.
func (a *Adapter) Set(ctx context.Context, entry *core.Entry) error {
	start := time.Now()
	defer func() {
		atomic.AddInt64(&a.setLatency, time.Since(start).Nanoseconds())
		atomic.AddUint64(&a.setCount, 1)
	}()

	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("not connected")
	}

	// 값 준비
	createdAt := entry.CreatedAt.Unix()
	var expiresAt *int64
	if !entry.ExpiresAt.IsZero() {
		ts := entry.ExpiresAt.Unix()
		expiresAt = &ts
	}
	lastAccessedAt := entry.LastAccessedAt.Unix()
	compressed := 0
	if entry.Compressed {
		compressed = 1
	}

	// UPSERT
	query := `
		INSERT INTO cache (key, value, created_at, expires_at, access_count, last_accessed_at, size, compressed, compression_type)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			created_at = excluded.created_at,
			expires_at = excluded.expires_at,
			access_count = excluded.access_count,
			last_accessed_at = excluded.last_accessed_at,
			size = excluded.size,
			compressed = excluded.compressed,
			compression_type = excluded.compression_type
	`
	_, err := db.ExecContext(ctx, query,
		entry.Key, entry.Value, createdAt, expiresAt, entry.AccessCount,
		lastAccessedAt, entry.Size, compressed, entry.CompressionType,
	)

	if err != nil {
		return fmt.Errorf("sqlite insert error: %w", err)
	}

	return nil
}

// Delete는 엔트리를 삭제합니다.
func (a *Adapter) Delete(ctx context.Context, key string) (bool, error) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return false, fmt.Errorf("not connected")
	}

	result, err := db.ExecContext(ctx, "DELETE FROM cache WHERE key = ?", key)
	if err != nil {
		return false, fmt.Errorf("sqlite delete error: %w", err)
	}

	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

// Has는 키가 존재하는지 확인합니다.
func (a *Adapter) Has(ctx context.Context, key string) (bool, error) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return false, fmt.Errorf("not connected")
	}

	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cache WHERE key = ?", key).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("sqlite count error: %w", err)
	}

	return count > 0, nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 배치 연산
// =============================================================================

// GetMany는 여러 키를 한 번에 조회합니다.
func (a *Adapter) GetMany(ctx context.Context, keys []string) (map[string]*core.Entry, error) {
	result := make(map[string]*core.Entry)

	for _, key := range keys {
		entry, err := a.Get(ctx, key)
		if err != nil {
			continue
		}
		if entry != nil {
			result[key] = entry
		}
	}

	return result, nil
}

// SetMany는 여러 엔트리를 한 번에 저장합니다.
func (a *Adapter) SetMany(ctx context.Context, entries []*core.Entry) error {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("not connected")
	}

	// 트랜잭션 시작
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	// Prepared statement
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO cache (key, value, created_at, expires_at, access_count, last_accessed_at, size, compressed, compression_type)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value = excluded.value,
			created_at = excluded.created_at,
			expires_at = excluded.expires_at,
			access_count = excluded.access_count,
			last_accessed_at = excluded.last_accessed_at,
			size = excluded.size,
			compressed = excluded.compressed,
			compression_type = excluded.compression_type
	`)
	if err != nil {
		return fmt.Errorf("prepare statement failed: %w", err)
	}
	defer stmt.Close()

	for _, entry := range entries {
		createdAt := entry.CreatedAt.Unix()
		var expiresAt *int64
		if !entry.ExpiresAt.IsZero() {
			ts := entry.ExpiresAt.Unix()
			expiresAt = &ts
		}
		lastAccessedAt := entry.LastAccessedAt.Unix()
		compressed := 0
		if entry.Compressed {
			compressed = 1
		}

		_, err := stmt.ExecContext(ctx,
			entry.Key, entry.Value, createdAt, expiresAt, entry.AccessCount,
			lastAccessedAt, entry.Size, compressed, entry.CompressionType,
		)
		if err != nil {
			return fmt.Errorf("insert failed: %w", err)
		}
	}

	return tx.Commit()
}

// DeleteMany는 여러 키를 한 번에 삭제합니다.
func (a *Adapter) DeleteMany(ctx context.Context, keys []string) (int, error) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return 0, fmt.Errorf("not connected")
	}

	// IN 절 생성
	args := make([]interface{}, len(keys))
	placeholders := ""
	for i, key := range keys {
		args[i] = key
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
	}

	query := fmt.Sprintf("DELETE FROM cache WHERE key IN (%s)", placeholders)
	result, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("sqlite delete error: %w", err)
	}

	affected, _ := result.RowsAffected()
	return int(affected), nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 관리
// =============================================================================

// Keys는 패턴에 매칭되는 모든 키를 반환합니다.
func (a *Adapter) Keys(ctx context.Context, pattern string) ([]string, error) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("not connected")
	}

	var query string
	var args []interface{}

	if pattern == "" || pattern == "*" {
		query = "SELECT key FROM cache"
	} else {
		// LIKE 패턴으로 변환
		likePattern := pattern
		likePattern = likePattern + "" // placeholder, implement proper glob-to-like conversion
		// 간단한 변환: * -> %, ? -> _
		for i := 0; i < len(likePattern); i++ {
			if likePattern[i] == '*' {
				likePattern = likePattern[:i] + "%" + likePattern[i+1:]
			} else if likePattern[i] == '?' {
				likePattern = likePattern[:i] + "_" + likePattern[i+1:]
			}
		}
		query = "SELECT key FROM cache WHERE key LIKE ?"
		args = append(args, likePattern)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite query error: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			continue
		}
		keys = append(keys, key)
	}

	return keys, nil
}

// Clear는 모든 엔트리를 삭제합니다.
func (a *Adapter) Clear(ctx context.Context) error {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("not connected")
	}

	_, err := db.ExecContext(ctx, "DELETE FROM cache")
	if err != nil {
		return fmt.Errorf("sqlite clear error: %w", err)
	}

	// VACUUM으로 공간 회수
	_, _ = db.ExecContext(ctx, "VACUUM")

	return nil
}

// Size는 저장된 엔트리 개수를 반환합니다.
func (a *Adapter) Size(ctx context.Context) (int64, error) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return 0, fmt.Errorf("not connected")
	}

	var count int64
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cache").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("sqlite count error: %w", err)
	}

	return count, nil
}

// =============================================================================
// Adapter 인터페이스 구현 - 메트릭
// =============================================================================

// Stats는 어댑터 통계를 반환합니다.
func (a *Adapter) Stats(ctx context.Context) (*core.AdapterStats, error) {
	getCount := atomic.LoadUint64(&a.getCount)
	setCount := atomic.LoadUint64(&a.setCount)

	var avgGetLatency, avgSetLatency int64
	if getCount > 0 {
		avgGetLatency = atomic.LoadInt64(&a.getLatency) / int64(getCount)
	}
	if setCount > 0 {
		avgSetLatency = atomic.LoadInt64(&a.setLatency) / int64(setCount)
	}

	size, _ := a.Size(ctx)

	// 파일 크기 확인
	var memoryUsage int64
	if a.config.Path != ":memory:" {
		if info, err := os.Stat(a.config.Path); err == nil {
			memoryUsage = info.Size()
		}
	}

	return &core.AdapterStats{
		Name:         a.Name(),
		Type:         a.Type(),
		Connected:    a.IsConnected(),
		Size:         size,
		MaxSize:      0, // SQLite는 제한 없음
		MemoryUsage:  memoryUsage,
		Hits:         atomic.LoadUint64(&a.hits),
		Misses:       atomic.LoadUint64(&a.misses),
		GetLatencyNs: avgGetLatency,
		SetLatencyNs: avgSetLatency,
		Evictions:    atomic.LoadUint64(&a.evictions),
	}, nil
}

// =============================================================================
// 내부 헬퍼
// =============================================================================

// cleanupLoop는 주기적으로 만료된 항목을 정리합니다.
func (a *Adapter) cleanupLoop() {
	for {
		select {
		case <-a.cleanupTicker.C:
			a.cleanupExpired()
		case <-a.closeCh:
			return
		}
	}
}

// cleanupExpired는 만료된 항목을 정리합니다.
func (a *Adapter) cleanupExpired() {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Now().Unix()
	result, err := db.ExecContext(ctx,
		"DELETE FROM cache WHERE expires_at IS NOT NULL AND expires_at < ?",
		now,
	)

	if err == nil {
		affected, _ := result.RowsAffected()
		atomic.AddUint64(&a.evictions, uint64(affected))
	}
}

// =============================================================================
// SQLite 전용 기능
// =============================================================================

// DB는 내부 데이터베이스 연결을 반환합니다.
func (a *Adapter) DB() *sql.DB {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.db
}

// Vacuum은 데이터베이스를 최적화합니다.
func (a *Adapter) Vacuum(ctx context.Context) error {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("not connected")
	}

	_, err := db.ExecContext(ctx, "VACUUM")
	return err
}

// ExportToJSON은 모든 엔트리를 JSON으로 내보냅니다.
func (a *Adapter) ExportToJSON(ctx context.Context) ([]byte, error) {
	keys, err := a.Keys(ctx, "*")
	if err != nil {
		return nil, err
	}

	entries := make([]*core.Entry, 0, len(keys))
	for _, key := range keys {
		entry, err := a.Get(ctx, key)
		if err == nil && entry != nil {
			entries = append(entries, entry)
		}
	}

	return json.MarshalIndent(entries, "", "  ")
}
