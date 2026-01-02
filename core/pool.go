// Package core는 HCache의 핵심 엔진을 구현합니다.
// 이 파일은 메모리 최적화를 위한 객체 풀을 구현합니다.
package core

import (
	"sync"
)

// =============================================================================
// EntryPool: Entry 객체 풀
// =============================================================================

// EntryPool은 Entry 객체를 재사용하는 풀입니다.
type EntryPool struct {
	pool sync.Pool
}

// 전역 Entry 풀
var globalEntryPool = &EntryPool{
	pool: sync.Pool{
		New: func() interface{} {
			return &Entry{
				Value: make([]byte, 0, 256),
			}
		},
	},
}

// NewEntryPool은 새로운 Entry 풀을 생성합니다.
func NewEntryPool() *EntryPool {
	return &EntryPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &Entry{}
			},
		},
	}
}

// Get은 풀에서 Entry를 가져옵니다.
func (p *EntryPool) Get() *Entry {
	return p.pool.Get().(*Entry)
}

// Put은 Entry를 풀에 반환합니다.
func (p *EntryPool) Put(e *Entry) {
	if e == nil {
		return
	}
	e.Key = ""
	e.Value = nil
	e.Size = 0
	e.AccessCount = 0
	e.Compressed = false
	e.CompressionType = ""
	p.pool.Put(e)
}

// GetEntry는 전역 풀에서 Entry를 가져옵니다.
func GetEntry() *Entry {
	return globalEntryPool.Get()
}

// PutEntry는 Entry를 전역 풀에 반환합니다.
func PutEntry(e *Entry) {
	globalEntryPool.Put(e)
}

// =============================================================================
// ByteSlicePool: 바이트 슬라이스 풀
// =============================================================================

// ByteSlicePool은 바이트 슬라이스를 재사용하는 풀입니다.
type ByteSlicePool struct {
	pool sync.Pool
	size int
}

// NewByteSlicePool은 새로운 바이트 슬라이스 풀을 생성합니다.
func NewByteSlicePool(size int) *ByteSlicePool {
	return &ByteSlicePool{
		size: size,
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, size)
			},
		},
	}
}

// Get은 풀에서 바이트 슬라이스를 가져옵니다.
func (p *ByteSlicePool) Get() []byte {
	return p.pool.Get().([]byte)[:0]
}

// Put은 바이트 슬라이스를 풀에 반환합니다.
func (p *ByteSlicePool) Put(b []byte) {
	if cap(b) == p.size {
		p.pool.Put(b[:0])
	}
}

// =============================================================================
// 크기별 버퍼 풀
// =============================================================================

var (
	smallBufPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 0, 256)
			return &b
		},
	}
	mediumBufPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 0, 4096)
			return &b
		},
	}
	largeBufPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 0, 65536)
			return &b
		},
	}
)

// GetBuffer는 필요한 크기에 맞는 버퍼를 풀에서 가져옵니다.
func GetBuffer(size int) *[]byte {
	if size <= 256 {
		return smallBufPool.Get().(*[]byte)
	} else if size <= 4096 {
		return mediumBufPool.Get().(*[]byte)
	} else if size <= 65536 {
		return largeBufPool.Get().(*[]byte)
	}
	b := make([]byte, 0, size)
	return &b
}

// PutBuffer는 버퍼를 풀에 반환합니다.
func PutBuffer(b *[]byte) {
	if b == nil {
		return
	}
	c := cap(*b)
	*b = (*b)[:0]

	if c == 256 {
		smallBufPool.Put(b)
	} else if c == 4096 {
		mediumBufPool.Put(b)
	} else if c == 65536 {
		largeBufPool.Put(b)
	}
}

// =============================================================================
// StringInterner: 문자열 인터닝
// =============================================================================

type StringInterner struct {
	strings sync.Map
	maxSize int
	size    int32
}

var globalInterner = &StringInterner{
	maxSize: 10000,
}

// InternString은 문자열을 인터닝합니다.
func InternString(s string) string {
	if v, ok := globalInterner.strings.Load(s); ok {
		return v.(string)
	}

	if globalInterner.size >= int32(globalInterner.maxSize) {
		return s
	}

	globalInterner.strings.Store(s, s)
	return s
}
