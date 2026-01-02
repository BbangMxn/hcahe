// Package plugin은 캐시 이벤트 플러그인 시스템을 구현합니다.
package plugin

import (
	"log"

	"github.com/bridgify/hcache/core"
)

// =============================================================================
// Plugin Interface
// =============================================================================

type Plugin interface {
	Name() string
	OnGet(key string, entry *core.Entry, layer int, hit bool)
	OnSet(key string, entry *core.Entry, layers []int)
	OnDelete(key string, layers []int)
	OnEviction(key string, layer int, reason string)
	OnPromotion(key string, from, to int)
	OnDemotion(key string, from, to int)
}

// =============================================================================
// BasePlugin
// =============================================================================

type BasePlugin struct {
	name string
}

func NewBase(name string) *BasePlugin {
	return &BasePlugin{name: name}
}

func (p *BasePlugin) Name() string                                             { return p.name }
func (p *BasePlugin) OnGet(key string, entry *core.Entry, layer int, hit bool) {}
func (p *BasePlugin) OnSet(key string, entry *core.Entry, layers []int)        {}
func (p *BasePlugin) OnDelete(key string, layers []int)                        {}
func (p *BasePlugin) OnEviction(key string, layer int, reason string)          {}
func (p *BasePlugin) OnPromotion(key string, from, to int)                     {}
func (p *BasePlugin) OnDemotion(key string, from, to int)                      {}

// =============================================================================
// Logging Plugin
// =============================================================================

type LoggingPlugin struct {
	*BasePlugin
	logger *log.Logger
	level  LogLevel
}

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

type LoggingOption func(*LoggingPlugin)

func WithLogger(logger *log.Logger) LoggingOption {
	return func(p *LoggingPlugin) {
		p.logger = logger
	}
}

func WithLogLevel(level LogLevel) LoggingOption {
	return func(p *LoggingPlugin) {
		p.level = level
	}
}

func NewLogging(opts ...LoggingOption) *LoggingPlugin {
	p := &LoggingPlugin{
		BasePlugin: NewBase("logging"),
		logger:     log.Default(),
		level:      LogLevelInfo,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *LoggingPlugin) OnGet(key string, entry *core.Entry, layer int, hit bool) {
	if p.level <= LogLevelDebug {
		if hit {
			p.logger.Printf("[CACHE] GET hit: key=%s layer=%d", key, layer)
		} else {
			p.logger.Printf("[CACHE] GET miss: key=%s", key)
		}
	}
}

func (p *LoggingPlugin) OnSet(key string, entry *core.Entry, layers []int) {
	if p.level <= LogLevelDebug {
		p.logger.Printf("[CACHE] SET: key=%s layers=%v size=%d", key, layers, entry.Size)
	}
}

func (p *LoggingPlugin) OnDelete(key string, layers []int) {
	if p.level <= LogLevelDebug {
		p.logger.Printf("[CACHE] DELETE: key=%s layers=%v", key, layers)
	}
}

func (p *LoggingPlugin) OnEviction(key string, layer int, reason string) {
	if p.level <= LogLevelInfo {
		p.logger.Printf("[CACHE] EVICTION: key=%s layer=%d reason=%s", key, layer, reason)
	}
}

func (p *LoggingPlugin) OnPromotion(key string, from, to int) {
	if p.level <= LogLevelInfo {
		p.logger.Printf("[CACHE] PROMOTION: key=%s from=%d to=%d", key, from, to)
	}
}

func (p *LoggingPlugin) OnDemotion(key string, from, to int) {
	if p.level <= LogLevelInfo {
		p.logger.Printf("[CACHE] DEMOTION: key=%s from=%d to=%d", key, from, to)
	}
}

// =============================================================================
// Callback Plugin
// =============================================================================

type CallbackPlugin struct {
	*BasePlugin
	onGet       func(key string, entry *core.Entry, layer int, hit bool)
	onSet       func(key string, entry *core.Entry, layers []int)
	onDelete    func(key string, layers []int)
	onEviction  func(key string, layer int, reason string)
	onPromotion func(key string, from, to int)
	onDemotion  func(key string, from, to int)
}

type CallbackOption func(*CallbackPlugin)

func OnGetCallback(fn func(key string, entry *core.Entry, layer int, hit bool)) CallbackOption {
	return func(p *CallbackPlugin) {
		p.onGet = fn
	}
}

func OnSetCallback(fn func(key string, entry *core.Entry, layers []int)) CallbackOption {
	return func(p *CallbackPlugin) {
		p.onSet = fn
	}
}

func OnDeleteCallback(fn func(key string, layers []int)) CallbackOption {
	return func(p *CallbackPlugin) {
		p.onDelete = fn
	}
}

func OnEvictionCallback(fn func(key string, layer int, reason string)) CallbackOption {
	return func(p *CallbackPlugin) {
		p.onEviction = fn
	}
}

func OnPromotionCallback(fn func(key string, from, to int)) CallbackOption {
	return func(p *CallbackPlugin) {
		p.onPromotion = fn
	}
}

func OnDemotionCallback(fn func(key string, from, to int)) CallbackOption {
	return func(p *CallbackPlugin) {
		p.onDemotion = fn
	}
}

func NewCallback(opts ...CallbackOption) *CallbackPlugin {
	p := &CallbackPlugin{
		BasePlugin: NewBase("callback"),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *CallbackPlugin) OnGet(key string, entry *core.Entry, layer int, hit bool) {
	if p.onGet != nil {
		p.onGet(key, entry, layer, hit)
	}
}

func (p *CallbackPlugin) OnSet(key string, entry *core.Entry, layers []int) {
	if p.onSet != nil {
		p.onSet(key, entry, layers)
	}
}

func (p *CallbackPlugin) OnDelete(key string, layers []int) {
	if p.onDelete != nil {
		p.onDelete(key, layers)
	}
}

func (p *CallbackPlugin) OnEviction(key string, layer int, reason string) {
	if p.onEviction != nil {
		p.onEviction(key, layer, reason)
	}
}

func (p *CallbackPlugin) OnPromotion(key string, from, to int) {
	if p.onPromotion != nil {
		p.onPromotion(key, from, to)
	}
}

func (p *CallbackPlugin) OnDemotion(key string, from, to int) {
	if p.onDemotion != nil {
		p.onDemotion(key, from, to)
	}
}
