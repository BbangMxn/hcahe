// Package serializer는 데이터 직렬화/역직렬화를 구현합니다.
package serializer

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

// =============================================================================
// Serializer Interface
// =============================================================================

// Serializer는 직렬화 인터페이스입니다.
type Serializer interface {
	Serialize(v interface{}) ([]byte, error)
	Deserialize(data []byte, v interface{}) error
	Name() string
}

// =============================================================================
// JSON Serializer
// =============================================================================

type JSONSerializer struct{}

func NewJSON() *JSONSerializer {
	return &JSONSerializer{}
}

func (s *JSONSerializer) Serialize(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json serialize error: %w", err)
	}
	return data, nil
}

func (s *JSONSerializer) Deserialize(data []byte, v interface{}) error {
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("json deserialize error: %w", err)
	}
	return nil
}

func (s *JSONSerializer) Name() string {
	return "json"
}

// =============================================================================
// MessagePack Serializer
// =============================================================================

type MsgPackSerializer struct{}

func NewMsgPack() *MsgPackSerializer {
	return &MsgPackSerializer{}
}

func (s *MsgPackSerializer) Serialize(v interface{}) ([]byte, error) {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("msgpack serialize error: %w", err)
	}
	return data, nil
}

func (s *MsgPackSerializer) Deserialize(data []byte, v interface{}) error {
	if err := msgpack.Unmarshal(data, v); err != nil {
		return fmt.Errorf("msgpack deserialize error: %w", err)
	}
	return nil
}

func (s *MsgPackSerializer) Name() string {
	return "msgpack"
}

// =============================================================================
// Gob Serializer
// =============================================================================

type GobSerializer struct{}

func NewGob() *GobSerializer {
	return &GobSerializer{}
}

func (s *GobSerializer) Serialize(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("gob serialize error: %w", err)
	}
	return buf.Bytes(), nil
}

func (s *GobSerializer) Deserialize(data []byte, v interface{}) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("gob deserialize error: %w", err)
	}
	return nil
}

func (s *GobSerializer) Name() string {
	return "gob"
}

// =============================================================================
// Raw Serializer
// =============================================================================

type RawSerializer struct{}

func NewRaw() *RawSerializer {
	return &RawSerializer{}
}

func (s *RawSerializer) Serialize(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	default:
		return nil, fmt.Errorf("raw serializer only supports []byte or string, got %T", v)
	}
}

func (s *RawSerializer) Deserialize(data []byte, v interface{}) error {
	switch ptr := v.(type) {
	case *[]byte:
		*ptr = data
		return nil
	case *string:
		*ptr = string(data)
		return nil
	default:
		return fmt.Errorf("raw deserializer only supports *[]byte or *string, got %T", v)
	}
}

func (s *RawSerializer) Name() string {
	return "raw"
}

// =============================================================================
// Factory
// =============================================================================

func New(name string) (Serializer, error) {
	switch name {
	case "json":
		return NewJSON(), nil
	case "msgpack":
		return NewMsgPack(), nil
	case "gob":
		return NewGob(), nil
	case "raw":
		return NewRaw(), nil
	default:
		return nil, fmt.Errorf("unknown serializer: %s", name)
	}
}
