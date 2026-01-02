package core

import (
	"testing"
	"time"
)

func TestNewEntry(t *testing.T) {
	entry := NewEntry("key", []byte("value"), 0)

	if entry.Key != "key" {
		t.Errorf("잘못된 키: %s", entry.Key)
	}
	if string(entry.Value) != "value" {
		t.Errorf("잘못된 값: %s", string(entry.Value))
	}
	if entry.Size != 5 {
		t.Errorf("잘못된 크기: %d", entry.Size)
	}
	if entry.AccessCount != 0 {
		t.Errorf("초기 접근 횟수가 0이 아닙니다: %d", entry.AccessCount)
	}
}

func TestEntryWithTTL(t *testing.T) {
	entry := NewEntry("key", []byte("value"), 1*time.Second)

	if entry.ExpiresAt.IsZero() {
		t.Error("TTL이 설정된 엔트리의 ExpiresAt이 Zero입니다")
	}

	// 아직 만료 안 됨
	if entry.IsExpired() {
		t.Error("아직 만료되지 않았는데 IsExpired가 true입니다")
	}

	// 만료 대기
	time.Sleep(1100 * time.Millisecond)

	if !entry.IsExpired() {
		t.Error("만료되었는데 IsExpired가 false입니다")
	}
}

func TestEntryNoTTL(t *testing.T) {
	entry := NewEntry("key", []byte("value"), 0)

	if !entry.ExpiresAt.IsZero() {
		t.Error("TTL이 없는 엔트리의 ExpiresAt이 Zero가 아닙니다")
	}

	if entry.IsExpired() {
		t.Error("TTL이 없는 엔트리가 만료되었다고 반환됨")
	}
}

func TestEntryTouch(t *testing.T) {
	entry := NewEntry("key", []byte("value"), 0)

	initialCount := entry.AccessCount
	entry.Touch()

	if entry.AccessCount != initialCount+1 {
		t.Errorf("Touch 후 AccessCount가 증가하지 않음: %d", entry.AccessCount)
	}
}

func TestEntryAge(t *testing.T) {
	entry := NewEntry("key", []byte("value"), 0)

	time.Sleep(100 * time.Millisecond)

	age := entry.Age()
	if age < 100*time.Millisecond {
		t.Errorf("Age가 예상보다 작습니다: %v", age)
	}
}

func TestEntryTTL(t *testing.T) {
	// TTL 없음
	entry1 := NewEntry("key1", []byte("value"), 0)
	if entry1.TTL() != -1 {
		t.Errorf("TTL 없는 엔트리의 TTL()이 -1이 아닙니다: %v", entry1.TTL())
	}

	// TTL 있음
	entry2 := NewEntry("key2", []byte("value"), 1*time.Second)
	ttl := entry2.TTL()
	if ttl <= 0 || ttl > 1*time.Second {
		t.Errorf("TTL이 예상 범위를 벗어남: %v", ttl)
	}

	// 만료 후
	time.Sleep(1100 * time.Millisecond)
	if entry2.TTL() != 0 {
		t.Errorf("만료된 엔트리의 TTL()이 0이 아닙니다: %v", entry2.TTL())
	}
}

func TestEntryIdleTime(t *testing.T) {
	entry := NewEntry("key", []byte("value"), 0)

	time.Sleep(100 * time.Millisecond)

	idle := entry.IdleTime()
	if idle < 100*time.Millisecond {
		t.Errorf("IdleTime이 예상보다 작습니다: %v", idle)
	}

	// Touch 후 IdleTime 리셋
	entry.Touch()
	idle = entry.IdleTime()
	if idle > 10*time.Millisecond {
		t.Errorf("Touch 후 IdleTime이 리셋되지 않음: %v", idle)
	}
}

func TestEntryClone(t *testing.T) {
	original := NewEntry("key", []byte("value"), 1*time.Minute)
	original.Touch()
	original.Touch()

	clone := original.Clone()

	// 값 비교
	if clone.Key != original.Key {
		t.Errorf("Clone 키가 다릅니다: %s != %s", clone.Key, original.Key)
	}
	if string(clone.Value) != string(original.Value) {
		t.Errorf("Clone 값이 다릅니다")
	}
	if clone.AccessCount != original.AccessCount {
		t.Errorf("Clone AccessCount가 다릅니다: %d != %d", clone.AccessCount, original.AccessCount)
	}

	// 독립성 확인 (원본 변경이 클론에 영향 안 줌)
	original.Value[0] = 'x'
	if clone.Value[0] == 'x' {
		t.Error("Clone이 원본과 값을 공유합니다")
	}
}

func TestEntryUpdateValue(t *testing.T) {
	entry := NewEntry("key", []byte("old"), 1*time.Minute)

	entry.UpdateValue([]byte("new value"), 2*time.Minute)

	if string(entry.Value) != "new value" {
		t.Errorf("값이 업데이트되지 않음: %s", string(entry.Value))
	}
	if entry.Size != 9 {
		t.Errorf("크기가 업데이트되지 않음: %d", entry.Size)
	}
}

func TestEntryMetrics(t *testing.T) {
	entry := NewEntry("key", []byte("value"), 1*time.Minute)
	entry.Touch()
	entry.Touch()
	entry.Touch()

	metrics := entry.Metrics()

	if metrics.Key != "key" {
		t.Errorf("Metrics 키가 다릅니다: %s", metrics.Key)
	}
	if metrics.Size != 5 {
		t.Errorf("Metrics 크기가 다릅니다: %d", metrics.Size)
	}
	if metrics.AccessCount != 3 {
		t.Errorf("Metrics AccessCount가 다릅니다: %d", metrics.AccessCount)
	}
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkNewEntry(b *testing.B) {
	value := []byte("benchmark value")
	for i := 0; i < b.N; i++ {
		NewEntry("key", value, time.Minute)
	}
}

func BenchmarkEntryTouch(b *testing.B) {
	entry := NewEntry("key", []byte("value"), 0)
	for i := 0; i < b.N; i++ {
		entry.Touch()
	}
}

func BenchmarkEntryIsExpired(b *testing.B) {
	entry := NewEntry("key", []byte("value"), time.Hour)
	for i := 0; i < b.N; i++ {
		entry.IsExpired()
	}
}

func BenchmarkEntryClone(b *testing.B) {
	entry := NewEntry("key", []byte("benchmark value for cloning"), time.Minute)
	for i := 0; i < b.N; i++ {
		entry.Clone()
	}
}
