// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 Write Buffer를 정의합니다.
package core

import (
	"sync"
	"time"
)

// =============================================================================
// WriteBuffer: 비동기 쓰기 버퍼
// =============================================================================
// WriteBuffer는 Hot/Warm 모드에서 하위 계층으로의 비동기 쓰기를 관리합니다.
// 버퍼가 가득 차거나 일정 시간이 지나면 자동으로 플러시됩니다.
//
// 설계 목표:
// - 락 최소화로 높은 동시성 지원
// - 같은 키에 대한 중복 쓰기 병합 (coalescing)
// - 배치 플러시로 네트워크 오버헤드 최소화
// =============================================================================

// BufferEntry는 버퍼에 저장되는 엔트리입니다.
type BufferEntry struct {
	Entry        *Entry
	TargetLayers []int
	Timestamp    time.Time
}

// FlushFunc는 버퍼 플러시 시 호출되는 함수입니다.
type FlushFunc func(entries []*BufferEntry)

// WriteBuffer는 비동기 쓰기 버퍼입니다.
type WriteBuffer struct {
	// size는 버퍼의 최대 크기입니다.
	size int

	// flushInterval은 자동 플러시 주기입니다.
	flushInterval time.Duration

	// flushFunc는 플러시 시 호출되는 함수입니다.
	flushFunc FlushFunc

	// buffer는 엔트리를 저장하는 맵입니다.
	// 같은 키의 중복 쓰기를 병합합니다.
	buffer map[string]*BufferEntry

	// mu는 버퍼 접근을 위한 뮤텍스입니다.
	mu sync.Mutex

	// inputCh는 새 엔트리를 받는 채널입니다.
	inputCh chan *BufferEntry

	// flushCh는 수동 플러시 요청을 받는 채널입니다.
	flushCh chan struct{}

	// closeCh는 종료 신호를 받는 채널입니다.
	closeCh chan struct{}

	// doneCh는 종료 완료를 알리는 채널입니다.
	doneCh chan struct{}

	// closed는 버퍼가 종료되었는지 여부입니다.
	closed bool
}

// =============================================================================
// WriteBuffer 생성자
// =============================================================================

// NewWriteBuffer는 새로운 WriteBuffer를 생성합니다.
//
// Parameters:
//   - size: 버퍼 최대 크기 (0이면 기본값 1000)
//   - flushInterval: 자동 플러시 주기 (0이면 기본값 100ms)
//   - flushFunc: 플러시 시 호출되는 함수
//
// Returns:
//   - *WriteBuffer: 생성된 버퍼
func NewWriteBuffer(size int, flushInterval time.Duration, flushFunc FlushFunc) *WriteBuffer {
	if size <= 0 {
		size = 1000
	}
	if flushInterval <= 0 {
		flushInterval = 100 * time.Millisecond
	}

	wb := &WriteBuffer{
		size:          size,
		flushInterval: flushInterval,
		flushFunc:     flushFunc,
		buffer:        make(map[string]*BufferEntry),
		inputCh:       make(chan *BufferEntry, size),
		flushCh:       make(chan struct{}, 1),
		closeCh:       make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	// 백그라운드 워커 시작
	go wb.worker()

	return wb
}

// =============================================================================
// WriteBuffer 메서드
// =============================================================================

// Add는 엔트리를 버퍼에 추가합니다.
// 버퍼가 가득 차면 자동으로 플러시됩니다.
//
// Parameters:
//   - entry: 저장할 엔트리
//   - targetLayers: 저장할 계층 인덱스들
func (wb *WriteBuffer) Add(entry *Entry, targetLayers []int) {
	if wb.closed {
		return
	}

	be := &BufferEntry{
		Entry:        entry.Clone(), // 복사본 저장 (동시성 안전)
		TargetLayers: targetLayers,
		Timestamp:    time.Now(),
	}

	// 논블로킹으로 채널에 추가
	select {
	case wb.inputCh <- be:
		// 성공
	default:
		// 채널이 가득 차면 강제 플러시
		wb.Flush()
		// 재시도
		select {
		case wb.inputCh <- be:
		default:
			// 여전히 실패하면 드롭 (로그 남기기)
		}
	}
}

// Flush는 버퍼를 수동으로 플러시합니다.
// 논블로킹으로 동작하며, 이미 플러시가 진행 중이면 무시됩니다.
func (wb *WriteBuffer) Flush() {
	if wb.closed {
		return
	}

	select {
	case wb.flushCh <- struct{}{}:
		// 플러시 요청 성공
	default:
		// 이미 플러시 요청이 있음 - 무시
	}
}

// Close는 버퍼를 종료합니다.
// 남은 엔트리를 모두 플러시하고 워커를 종료합니다.
func (wb *WriteBuffer) Close() {
	if wb.closed {
		return
	}
	wb.closed = true

	// 종료 신호 전송
	close(wb.closeCh)

	// 워커 종료 대기
	<-wb.doneCh
}

// Size는 현재 버퍼에 있는 엔트리 수를 반환합니다.
func (wb *WriteBuffer) Size() int {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return len(wb.buffer)
}

// Pending은 채널에 대기 중인 엔트리 수를 반환합니다.
func (wb *WriteBuffer) Pending() int {
	return len(wb.inputCh)
}

// =============================================================================
// WriteBuffer 백그라운드 워커
// =============================================================================

// worker는 백그라운드에서 버퍼를 관리합니다.
// 세 가지 이벤트를 처리합니다:
// 1. 새 엔트리 추가
// 2. 주기적 플러시
// 3. 수동 플러시 요청
// 4. 종료 신호
func (wb *WriteBuffer) worker() {
	defer close(wb.doneCh)

	ticker := time.NewTicker(wb.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case be := <-wb.inputCh:
			// 새 엔트리 추가
			wb.mu.Lock()

			// 같은 키면 덮어쓰기 (중복 쓰기 병합)
			wb.buffer[be.Entry.Key] = be

			// 버퍼가 가득 차면 플러시
			shouldFlush := len(wb.buffer) >= wb.size
			wb.mu.Unlock()

			if shouldFlush {
				wb.doFlush()
			}

		case <-ticker.C:
			// 주기적 플러시
			wb.doFlush()

		case <-wb.flushCh:
			// 수동 플러시
			wb.doFlush()

		case <-wb.closeCh:
			// 종료 - 남은 엔트리 모두 처리

			// 1. 채널에 남은 엔트리 버퍼로 이동
			wb.mu.Lock()
		drainLoop:
			for {
				select {
				case be := <-wb.inputCh:
					wb.buffer[be.Entry.Key] = be
				default:
					break drainLoop
				}
			}
			wb.mu.Unlock()

			// 2. 최종 플러시
			wb.doFlush()
			return
		}
	}
}

// doFlush는 버퍼의 모든 엔트리를 플러시합니다.
func (wb *WriteBuffer) doFlush() {
	wb.mu.Lock()

	if len(wb.buffer) == 0 {
		wb.mu.Unlock()
		return
	}

	// 버퍼 내용을 슬라이스로 변환
	entries := make([]*BufferEntry, 0, len(wb.buffer))
	for _, be := range wb.buffer {
		entries = append(entries, be)
	}

	// 버퍼 초기화
	wb.buffer = make(map[string]*BufferEntry)

	wb.mu.Unlock()

	// 플러시 함수 호출 (비동기)
	if wb.flushFunc != nil {
		go wb.flushFunc(entries)
	}
}

// =============================================================================
// WriteBuffer 통계
// =============================================================================

// Stats는 버퍼의 통계를 반환합니다.
type WriteBufferStats struct {
	Size          int `json:"size"`
	MaxSize       int `json:"max_size"`
	Pending       int `json:"pending"`
	FlushInterval int `json:"flush_interval_ms"`
}

// Stats는 버퍼의 통계를 반환합니다.
func (wb *WriteBuffer) Stats() WriteBufferStats {
	return WriteBufferStats{
		Size:          wb.Size(),
		MaxSize:       wb.size,
		Pending:       wb.Pending(),
		FlushInterval: int(wb.flushInterval.Milliseconds()),
	}
}
