// Package compression은 데이터 압축/해제를 구현합니다.
package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// =============================================================================
// Compressor Interface
// =============================================================================

// Compressor는 압축 인터페이스입니다.
type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
	Name() string
}

// =============================================================================
// Gzip Compressor
// =============================================================================

type GzipCompressor struct {
	level int
}

type GzipOption func(*GzipCompressor)

func WithGzipLevel(level int) GzipOption {
	return func(c *GzipCompressor) {
		c.level = level
	}
}

func NewGzip(opts ...GzipOption) *GzipCompressor {
	c := &GzipCompressor{
		level: gzip.DefaultCompression,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *GzipCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, c.level)
	if err != nil {
		return nil, fmt.Errorf("gzip writer error: %w", err)
	}

	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, fmt.Errorf("gzip write error: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip close error: %w", err)
	}

	return buf.Bytes(), nil
}

func (c *GzipCompressor) Decompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip reader error: %w", err)
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("gzip read error: %w", err)
	}

	return result, nil
}

func (c *GzipCompressor) Name() string {
	return "gzip"
}

// =============================================================================
// S2 Compressor (LZ4 대체, 더 빠름)
// =============================================================================

type S2Compressor struct{}

func NewS2() *S2Compressor {
	return &S2Compressor{}
}

// NewLZ4는 S2 압축기를 반환합니다 (LZ4 호환 대체)
func NewLZ4() *S2Compressor {
	return &S2Compressor{}
}

func (c *S2Compressor) Compress(data []byte) ([]byte, error) {
	return s2.Encode(nil, data), nil
}

func (c *S2Compressor) Decompress(data []byte) ([]byte, error) {
	return s2.Decode(nil, data)
}

func (c *S2Compressor) Name() string {
	return "s2"
}

// =============================================================================
// Zstd Compressor
// =============================================================================

type ZstdCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func NewZstd() (*ZstdCompressor, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("zstd encoder error: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("zstd decoder error: %w", err)
	}

	return &ZstdCompressor{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (c *ZstdCompressor) Compress(data []byte) ([]byte, error) {
	return c.encoder.EncodeAll(data, nil), nil
}

func (c *ZstdCompressor) Decompress(data []byte) ([]byte, error) {
	result, err := c.decoder.DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("zstd decode error: %w", err)
	}
	return result, nil
}

func (c *ZstdCompressor) Name() string {
	return "zstd"
}

func (c *ZstdCompressor) Close() {
	if c.encoder != nil {
		c.encoder.Close()
	}
	if c.decoder != nil {
		c.decoder.Close()
	}
}

// =============================================================================
// No-op Compressor
// =============================================================================

type NoopCompressor struct{}

func NewNoop() *NoopCompressor {
	return &NoopCompressor{}
}

func (c *NoopCompressor) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (c *NoopCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

func (c *NoopCompressor) Name() string {
	return "none"
}

// =============================================================================
// Factory
// =============================================================================

func New(name string) (Compressor, error) {
	switch name {
	case "gzip":
		return NewGzip(), nil
	case "lz4", "s2":
		return NewS2(), nil
	case "zstd":
		return NewZstd()
	case "none", "":
		return NewNoop(), nil
	default:
		return nil, fmt.Errorf("unknown compressor: %s", name)
	}
}

// =============================================================================
// Adaptive Compressor
// =============================================================================

type AdaptiveCompressor struct {
	compressor Compressor
	threshold  int
}

func NewAdaptive(compressor Compressor, threshold int) *AdaptiveCompressor {
	return &AdaptiveCompressor{
		compressor: compressor,
		threshold:  threshold,
	}
}

func (c *AdaptiveCompressor) Compress(data []byte) ([]byte, error) {
	if len(data) < c.threshold {
		return data, nil
	}
	return c.compressor.Compress(data)
}

func (c *AdaptiveCompressor) Decompress(data []byte) ([]byte, error) {
	result, err := c.compressor.Decompress(data)
	if err != nil {
		return data, nil
	}
	return result, nil
}

func (c *AdaptiveCompressor) Name() string {
	return c.compressor.Name() + "-adaptive"
}

// =============================================================================
// Compression Stats
// =============================================================================

type CompressionStats struct {
	OriginalSize   int
	CompressedSize int
	Ratio          float64
	SavedBytes     int
	SavedPercent   float64
}

func Analyze(data []byte, compressor Compressor) (*CompressionStats, error) {
	compressed, err := compressor.Compress(data)
	if err != nil {
		return nil, err
	}

	originalSize := len(data)
	compressedSize := len(compressed)
	savedBytes := originalSize - compressedSize

	return &CompressionStats{
		OriginalSize:   originalSize,
		CompressedSize: compressedSize,
		Ratio:          float64(compressedSize) / float64(originalSize),
		SavedBytes:     savedBytes,
		SavedPercent:   float64(savedBytes) / float64(originalSize) * 100,
	}, nil
}
