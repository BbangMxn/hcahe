package core

import (
	"context"
	"testing"
	"time"
)

// =============================================================================
// Mock Adapter for Testing
// =============================================================================

type mockAdapter struct {
	data      map[string]*Entry
	connected bool
}

func newMockAdapter() *mockAdapter {
	return &mockAdapter{
		data: make(map[string]*Entry),
	}
}

func (m *mockAdapter) Name() string                         { return "mock" }
func (m *mockAdapter) Type() AdapterType                    { return TypeMemory }
func (m *mockAdapter) Connect(ctx context.Context) error    { m.connected = true; return nil }
func (m *mockAdapter) Disconnect(ctx context.Context) error { m.connected = false; return nil }
func (m *mockAdapter) IsConnected() bool                    { return m.connected }
func (m *mockAdapter) Ping(ctx context.Context) error       { return nil }

func (m *mockAdapter) Get(ctx context.Context, key string) (*Entry, error) {
	entry, exists := m.data[key]
	if !exists {
		return nil, nil
	}
	return entry, nil
}

func (m *mockAdapter) Set(ctx context.Context, entry *Entry) error {
	m.data[entry.Key] = entry
	return nil
}

func (m *mockAdapter) Delete(ctx context.Context, key string) (bool, error) {
	if _, exists := m.data[key]; exists {
		delete(m.data, key)
		return true, nil
	}
	return false, nil
}

func (m *mockAdapter) Has(ctx context.Context, key string) (bool, error) {
	_, exists := m.data[key]
	return exists, nil
}

func (m *mockAdapter) GetMany(ctx context.Context, keys []string) (map[string]*Entry, error) {
	result := make(map[string]*Entry)
	for _, key := range keys {
		if entry, exists := m.data[key]; exists {
			result[key] = entry
		}
	}
	return result, nil
}

func (m *mockAdapter) SetMany(ctx context.Context, entries []*Entry) error {
	for _, entry := range entries {
		m.data[entry.Key] = entry
	}
	return nil
}

func (m *mockAdapter) DeleteMany(ctx context.Context, keys []string) (int, error) {
	count := 0
	for _, key := range keys {
		if _, exists := m.data[key]; exists {
			delete(m.data, key)
			count++
		}
	}
	return count, nil
}

func (m *mockAdapter) Keys(ctx context.Context, pattern string) ([]string, error) {
	keys := make([]string, 0, len(m.data))
	for key := range m.data {
		keys = append(keys, key)
	}
	return keys, nil
}

func (m *mockAdapter) Clear(ctx context.Context) error {
	m.data = make(map[string]*Entry)
	return nil
}

func (m *mockAdapter) Size(ctx context.Context) (int64, error) {
	return int64(len(m.data)), nil
}

func (m *mockAdapter) Stats(ctx context.Context) (*AdapterStats, error) {
	return &AdapterStats{
		Name:      m.Name(),
		Type:      m.Type(),
		Connected: m.connected,
		Size:      int64(len(m.data)),
	}, nil
}

// =============================================================================
// Tests
// =============================================================================

func TestNewCache(t *testing.T) {
	cache := New()
	if cache == nil {
		t.Fatal("캐시가 nil입니다")
	}
	if cache.LayerCount() != 0 {
		t.Errorf("초기 계층 수가 0이 아닙니다: %d", cache.LayerCount())
	}
}

func TestCacheWithOptions(t *testing.T) {
	cache := New(
		WithDefaultTTL(10*time.Minute),
		WithDefaultWriteMode(Warm),
	)

	if cache == nil {
		t.Fatal("캐시가 nil입니다")
	}
}

func TestAddLayer(t *testing.T) {
	cache := New()
	adapter := newMockAdapter()

	err := cache.AddLayer(adapter, &LayerOptions{
		Name:     "test",
		Priority: 0,
	})

	if err != nil {
		t.Fatalf("계층 추가 실패: %v", err)
	}

	if cache.LayerCount() != 1 {
		t.Errorf("계층 수가 1이 아닙니다: %d", cache.LayerCount())
	}
}

func TestSetAndGetRaw(t *testing.T) {
	ctx := context.Background()
	cache := New()
	adapter := newMockAdapter()

	cache.AddLayer(adapter, &LayerOptions{
		Name:     "test",
		Priority: 0,
	})
	cache.Connect(ctx)

	// Entry 직접 저장
	entry := NewEntry("key1", []byte("value1"), 0)
	err := cache.SetRaw(ctx, entry)
	if err != nil {
		t.Fatalf("SetRaw 실패: %v", err)
	}

	// 조회
	result, layer, err := cache.GetRaw(ctx, "key1")
	if err != nil {
		t.Fatalf("GetRaw 실패: %v", err)
	}
	if layer != 0 {
		t.Errorf("잘못된 계층: %d", layer)
	}
	if string(result.Value) != "value1" {
		t.Errorf("잘못된 값: %s", string(result.Value))
	}
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	cache := New()
	adapter := newMockAdapter()

	cache.AddLayer(adapter, &LayerOptions{
		Name:     "test",
		Priority: 0,
	})
	cache.Connect(ctx)

	// 저장
	entry := NewEntry("key1", []byte("value1"), 0)
	cache.SetRaw(ctx, entry)

	// 삭제
	err := cache.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("Delete 실패: %v", err)
	}

	// 확인
	exists, _ := cache.Has(ctx, "key1")
	if exists {
		t.Error("삭제 후에도 키가 존재합니다")
	}
}

func TestHas(t *testing.T) {
	ctx := context.Background()
	cache := New()
	adapter := newMockAdapter()

	cache.AddLayer(adapter, &LayerOptions{
		Name:     "test",
		Priority: 0,
	})
	cache.Connect(ctx)

	// 없는 키
	exists, _ := cache.Has(ctx, "nonexistent")
	if exists {
		t.Error("존재하지 않는 키가 존재한다고 반환됨")
	}

	// 저장 후
	entry := NewEntry("key1", []byte("value1"), 0)
	cache.SetRaw(ctx, entry)

	exists, _ = cache.Has(ctx, "key1")
	if !exists {
		t.Error("존재하는 키가 존재하지 않는다고 반환됨")
	}
}

func TestClear(t *testing.T) {
	ctx := context.Background()
	cache := New()
	adapter := newMockAdapter()

	cache.AddLayer(adapter, &LayerOptions{
		Name:     "test",
		Priority: 0,
	})
	cache.Connect(ctx)

	// 여러 개 저장
	for i := 0; i < 10; i++ {
		entry := NewEntry("key"+string(rune('0'+i)), []byte("value"), 0)
		cache.SetRaw(ctx, entry)
	}

	// 전체 삭제
	err := cache.Clear(ctx)
	if err != nil {
		t.Fatalf("Clear 실패: %v", err)
	}

	// 확인
	size, _ := adapter.Size(ctx)
	if size != 0 {
		t.Errorf("Clear 후 크기가 0이 아닙니다: %d", size)
	}
}

func TestMultipleLayers(t *testing.T) {
	ctx := context.Background()
	cache := New()

	// L1
	adapter1 := newMockAdapter()
	cache.AddLayer(adapter1, &LayerOptions{
		Name:     "L1",
		Priority: 0,
	})

	// L2
	adapter2 := newMockAdapter()
	cache.AddLayer(adapter2, &LayerOptions{
		Name:     "L2",
		Priority: 1,
	})

	cache.Connect(ctx)

	if cache.LayerCount() != 2 {
		t.Errorf("계층 수가 2가 아닙니다: %d", cache.LayerCount())
	}

	// L1에만 저장
	entry := NewEntry("key1", []byte("value1"), 0)
	adapter1.Set(ctx, entry)

	// 조회 시 L1에서 찾아야 함
	result, layer, _ := cache.GetRaw(ctx, "key1")
	if layer != 0 {
		t.Errorf("L1에서 찾아야 하는데 L%d에서 찾음", layer)
	}
	if result == nil {
		t.Error("결과가 nil입니다")
	}
}

func TestLayerPriorityOrder(t *testing.T) {
	cache := New()

	// 순서 뒤섞어서 추가
	cache.AddLayer(newMockAdapter(), &LayerOptions{Name: "L2", Priority: 2})
	cache.AddLayer(newMockAdapter(), &LayerOptions{Name: "L0", Priority: 0})
	cache.AddLayer(newMockAdapter(), &LayerOptions{Name: "L1", Priority: 1})

	layers := cache.Layers()

	// Priority 순으로 정렬되어야 함
	if layers[0].Name() != "L0" {
		t.Errorf("첫 번째 계층이 L0이 아닙니다: %s", layers[0].Name())
	}
	if layers[1].Name() != "L1" {
		t.Errorf("두 번째 계층이 L1이 아닙니다: %s", layers[1].Name())
	}
	if layers[2].Name() != "L2" {
		t.Errorf("세 번째 계층이 L2이 아닙니다: %s", layers[2].Name())
	}
}

func TestStats(t *testing.T) {
	ctx := context.Background()
	cache := New()
	adapter := newMockAdapter()

	cache.AddLayer(adapter, &LayerOptions{
		Name:     "test",
		Priority: 0,
	})
	cache.Connect(ctx)

	stats, err := cache.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats 실패: %v", err)
	}

	if len(stats) != 1 {
		t.Errorf("통계 개수가 1이 아닙니다: %d", len(stats))
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkSetRaw(b *testing.B) {
	ctx := context.Background()
	cache := New()
	adapter := newMockAdapter()
	cache.AddLayer(adapter, &LayerOptions{Name: "test", Priority: 0})
	cache.Connect(ctx)

	entry := NewEntry("key", []byte("value"), 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.SetRaw(ctx, entry)
	}
}

func BenchmarkGetRaw(b *testing.B) {
	ctx := context.Background()
	cache := New()
	adapter := newMockAdapter()
	cache.AddLayer(adapter, &LayerOptions{Name: "test", Priority: 0})
	cache.Connect(ctx)

	entry := NewEntry("key", []byte("value"), 0)
	cache.SetRaw(ctx, entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.GetRaw(ctx, "key")
	}
}

func BenchmarkHas(b *testing.B) {
	ctx := context.Background()
	cache := New()
	adapter := newMockAdapter()
	cache.AddLayer(adapter, &LayerOptions{Name: "test", Priority: 0})
	cache.Connect(ctx)

	entry := NewEntry("key", []byte("value"), 0)
	cache.SetRaw(ctx, entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Has(ctx, "key")
	}
}
