package singleflight

import "sync"

func New[T comparable]() *Group[T] {
	return &Group[T]{
		calls: make(map[T]struct{}),
	}
}

type Group[T comparable] struct {
	mu    sync.RWMutex
	calls map[T]struct{}
}

// TryDo will execute the function if the key is not locked
func (g *Group[T]) TryDo(key T, fn func() error) error {
	if g.isLocked(key) {
		return nil
	}

	if !g.lock(key) {
		return nil
	}
	defer g.unlock(key)

	return fn()
}

func (g *Group[T]) isLocked(key T) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	_, isLocked := g.calls[key]
	return isLocked
}

// lock returns true if the lock was performed
func (g *Group[T]) lock(key T) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	_, wasLocked := g.calls[key]
	if wasLocked {
		return false
	}

	g.calls[key] = struct{}{}
	return true
}

func (g *Group[T]) unlock(key T) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.calls, key)
}
