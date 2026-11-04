package idempotency

import (
	"context"
	"sync"
	"time"
)

type state uint8

const (
	stateProcessing state = iota
	stateCompleted
)

type entry struct {
	state     state
	expiresAt time.Time
}

type Memory struct {
	mu      sync.Mutex
	entries map[string]entry
	now     func() time.Time
}

func NewMemory() *Memory {
	return &Memory{entries: make(map[string]entry), now: time.Now}
}

func (m *Memory) Acquire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if e, ok := m.entries[key]; ok && now.Before(e.expiresAt) {
		return false, nil
	}
	m.entries[key] = entry{state: stateProcessing, expiresAt: now.Add(ttl)}
	return true, nil
}

func (m *Memory) Complete(_ context.Context, key string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[key] = entry{state: stateCompleted, expiresAt: m.now().Add(ttl)}
	return nil
}

func (m *Memory) Release(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok && e.state == stateProcessing {
		delete(m.entries, key)
	}
	return nil
}
