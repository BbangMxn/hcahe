// Package postgres는 PostgreSQL 캐시 어댑터를 구현합니다.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bridgify/hcache/core"
	_ "github.com/lib/pq"
)

// Config는 PostgreSQL 어댑터 설정입니다.
type Config struct {
	// DSN은 PostgreSQL 연결 문자열입니다.
	DSN string

	// TableName은 캐시 테이블 이름입니다.
	TableName string

	// MaxOpenConns는 최대 열린 연결 수입니다.
	MaxOpenConns int

	// MaxIdleConns는 최대 유휴 연결 수입니다.
	MaxIdleConns int

	// ConnMaxLifetime은 연결의 최대 수명입니다.
	ConnMaxLifetime time.Duration

	// CleanupInterval은 만료된 항목 정리 주기입니다.
	CleanupInterval time.Duration
}

// DefaultConfig는 기본 설정을 반환합니다.
func DefaultConfig() *Config {
	return &Config{
		TableName:       "hcache",
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 5 * time.Minute,
		CleanupInterval: 5 * time.Minute,
	}
}

// Adapter는 PostgreSQL 캐시 어댑터입니다.
type Adapter struct {
	config    *Config
	db        *sql.DB
	connected bool

	// 메트릭
	hits       uint64
	misses     uint64
	getLatency int64
	getCount   uint64
	setLatency int64
	setCount   uint64

	// cleanup
	cleanupTicker *time.Ticker
	closeCh       chan struct{}

	mu sync.RWMutex
}

// New는 새로운 PostgreSQL 어댑터를 생성합니다.
func New(config *Config) *Adapter {
	if config == nil {
		config = DefaultConfig()
	}
	if config.TableName == "" {
		config.TableName = "hcache"
	}

	return &Adapter{
		config:  config,
		closeCh: make(chan struct{}),
	}
}

func (a *Adapter) Name() string           { return "postgres" }
func (a *Adapter) Type() core.AdapterType { return core.TypeDatabase }

func (a *Adapter) Connect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.connected {
		return nil
	}

	db, err := sql.Open("postgres", a.config.DSN)
	if err != nil {
		return fmt.Errorf("postgres open failed: %w", err)
	}

	db.SetMaxOpenConns(a.config.MaxOpenConns)
	db.SetMaxIdleConns(a.config.MaxIdleConns)
	db.SetConnMaxLifetime(a.config.ConnMaxLifetime)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("postgres ping failed: %w", err)
	}

	// 테이블 생성
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key TEXT PRIMARY KEY,
			value BYTEA NOT NULL,
			created_at BIGINT NOT NULL,
			expires_at BIGINT,
			access_count BIGINT DEFAULT 0,
			last_accessed_at BIGINT,
			size INTEGER,
			compressed BOOLEAN DEFAULT FALSE,
			compression_type TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_%s_expires_at ON %s(expires_at);
	`, a.config.TableName, a.config.TableName, a.config.TableName)

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

func (a *Adapter) Disconnect(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.connected || a.db == nil {
		return nil
	}

	if a.cleanupTicker != nil {
		a.cleanupTicker.Stop()
		close(a.closeCh)
	}

	err := a.db.Close()
	a.connected = false
	a.db = nil
	return err
}

func (a *Adapter) IsConnected() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.connected
}

func (a *Adapter) Ping(ctx context.Context) error {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("not connected")
	}
	return db.PingContext(ctx)
}

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

	var value []byte
	var createdAt, expiresAt, lastAccessedAt sql.NullInt64
	var accessCount int64
	var size int
	var compressed bool
	var compressionType sql.NullString

	query := fmt.Sprintf(`
		SELECT value, created_at, expires_at, access_count, last_accessed_at, size, compressed, compression_type
		FROM %s WHERE key = $1
	`, a.config.TableName)

	err := db.QueryRowContext(ctx, query, key).Scan(
		&value, &createdAt, &expiresAt, &accessCount, &lastAccessedAt,
		&size, &compressed, &compressionType,
	)

	if err == sql.ErrNoRows {
		atomic.AddUint64(&a.misses, 1)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres query error: %w", err)
	}

	entry := &core.Entry{
		Key:         key,
		Value:       value,
		CreatedAt:   time.Unix(createdAt.Int64, 0),
		AccessCount: uint64(accessCount),
		Size:        size,
		Compressed:  compressed,
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

	// 만료 확인
	if entry.IsExpired() {
		go func() {
			delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a.Delete(delCtx, key)
		}()
		atomic.AddUint64(&a.misses, 1)
		return nil, nil
	}

	atomic.AddUint64(&a.hits, 1)
	return entry, nil
}

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

	createdAt := entry.CreatedAt.Unix()
	var expiresAt *int64
	if !entry.ExpiresAt.IsZero() {
		ts := entry.ExpiresAt.Unix()
		expiresAt = &ts
	}
	lastAccessedAt := entry.LastAccessedAt.Unix()

	query := fmt.Sprintf(`
		INSERT INTO %s (key, value, created_at, expires_at, access_count, last_accessed_at, size, compressed, compression_type)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT(key) DO UPDATE SET
			value = EXCLUDED.value,
			created_at = EXCLUDED.created_at,
			expires_at = EXCLUDED.expires_at,
			access_count = EXCLUDED.access_count,
			last_accessed_at = EXCLUDED.last_accessed_at,
			size = EXCLUDED.size,
			compressed = EXCLUDED.compressed,
			compression_type = EXCLUDED.compression_type
	`, a.config.TableName)

	_, err := db.ExecContext(ctx, query,
		entry.Key, entry.Value, createdAt, expiresAt, entry.AccessCount,
		lastAccessedAt, entry.Size, entry.Compressed, entry.CompressionType,
	)

	if err != nil {
		return fmt.Errorf("postgres insert error: %w", err)
	}

	return nil
}

func (a *Adapter) Delete(ctx context.Context, key string) (bool, error) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return false, fmt.Errorf("not connected")
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE key = $1", a.config.TableName)
	result, err := db.ExecContext(ctx, query, key)
	if err != nil {
		return false, fmt.Errorf("postgres delete error: %w", err)
	}

	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

func (a *Adapter) Has(ctx context.Context, key string) (bool, error) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return false, fmt.Errorf("not connected")
	}

	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE key = $1", a.config.TableName)
	err := db.QueryRowContext(ctx, query, key).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("postgres count error: %w", err)
	}

	return count > 0, nil
}

func (a *Adapter) GetMany(ctx context.Context, keys []string) (map[string]*core.Entry, error) {
	result := make(map[string]*core.Entry)
	for _, key := range keys {
		entry, err := a.Get(ctx, key)
		if err == nil && entry != nil {
			result[key] = entry
		}
	}
	return result, nil
}

func (a *Adapter) SetMany(ctx context.Context, entries []*core.Entry) error {
	for _, entry := range entries {
		if err := a.Set(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

func (a *Adapter) DeleteMany(ctx context.Context, keys []string) (int, error) {
	count := 0
	for _, key := range keys {
		if deleted, _ := a.Delete(ctx, key); deleted {
			count++
		}
	}
	return count, nil
}

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
		query = fmt.Sprintf("SELECT key FROM %s", a.config.TableName)
	} else {
		// LIKE 패턴으로 변환
		likePattern := pattern
		for i := 0; i < len(likePattern); i++ {
			if likePattern[i] == '*' {
				likePattern = likePattern[:i] + "%" + likePattern[i+1:]
			}
		}
		query = fmt.Sprintf("SELECT key FROM %s WHERE key LIKE $1", a.config.TableName)
		args = append(args, likePattern)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres query error: %w", err)
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

func (a *Adapter) Clear(ctx context.Context) error {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return fmt.Errorf("not connected")
	}

	query := fmt.Sprintf("DELETE FROM %s", a.config.TableName)
	_, err := db.ExecContext(ctx, query)
	return err
}

func (a *Adapter) Size(ctx context.Context) (int64, error) {
	a.mu.RLock()
	db := a.db
	a.mu.RUnlock()

	if db == nil {
		return 0, fmt.Errorf("not connected")
	}

	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", a.config.TableName)
	err := db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

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

	return &core.AdapterStats{
		Name:         a.Name(),
		Type:         a.Type(),
		Connected:    a.IsConnected(),
		Size:         size,
		Hits:         atomic.LoadUint64(&a.hits),
		Misses:       atomic.LoadUint64(&a.misses),
		GetLatencyNs: avgGetLatency,
		SetLatencyNs: avgSetLatency,
	}, nil
}

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
	query := fmt.Sprintf("DELETE FROM %s WHERE expires_at IS NOT NULL AND expires_at < $1", a.config.TableName)
	db.ExecContext(ctx, query, now)
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
