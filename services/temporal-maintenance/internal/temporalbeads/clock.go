package temporalbeads

import (
	"fmt"
	"sync"
	"time"
)

// Clock supplies deterministic time at IO boundaries.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now().UTC()
}

// NewSystemClock returns the production wall clock for IO boundaries.
func NewSystemClock() Clock {
	return realClock{}
}

// ManualClock is a deterministic test clock.
type ManualClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewManualClock starts a deterministic clock at now.
func NewManualClock(now time.Time) *ManualClock {
	return &ManualClock{now: now.UTC()}
}

// Now returns the current deterministic instant.
func (c *ManualClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Advance moves the deterministic clock forward.
func (c *ManualClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

// TimingConfig centralizes configurable Activity and reconciliation timing.
type TimingConfig struct {
	HeartbeatTimeout  time.Duration
	ReconcileInterval time.Duration
	Clock             Clock
}

// Validate rejects missing or non-positive boundary timing.
func (c TimingConfig) Validate() error {
	if c.HeartbeatTimeout <= 0 {
		return fmt.Errorf("heartbeat timeout must be positive")
	}
	if c.ReconcileInterval <= 0 {
		return fmt.Errorf("reconcile interval must be positive")
	}
	if c.Clock == nil {
		return fmt.Errorf("clock is required")
	}
	return nil
}

// Now reads the configured clock.
func (c TimingConfig) Now() time.Time {
	return c.Clock.Now().UTC()
}

func defaultTimingConfig() TimingConfig {
	return TimingConfig{
		HeartbeatTimeout:  time.Minute,
		ReconcileInterval: time.Minute,
		Clock:             realClock{},
	}
}
