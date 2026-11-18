package cache

import (
	"context"
	"sync"
	"time"

	"lastserver/internal/model"
)

type entry struct {
	product model.Product
	expires time.Time
}

type Products struct {
	mu      sync.Mutex
	entries map[string]entry
	epoch   uint64
	max     int
	ttl     time.Duration
}

func NewProducts(max int, ttl time.Duration) *Products {
	if ttl > 2*time.Second {
		ttl = 2 * time.Second
	}
	return &Products{entries: make(map[string]entry), max: max, ttl: ttl}
}

func (c *Products) Invalidate(id string) {
	c.mu.Lock()
	c.epoch++
	delete(c.entries, id)
	c.mu.Unlock()
}

func (c *Products) Get(ctx context.Context, id string, load func(context.Context, string) (model.Product, error)) (model.Product, error) {
	c.mu.Lock()
	epoch := c.epoch
	if e, ok := c.entries[id]; ok && time.Now().Before(e.expires) {
		c.mu.Unlock()
		return e.product, nil
	}
	c.mu.Unlock()
	started := time.Now()
	p, err := load(ctx, id)
	if err != nil {
		return p, err
	}
	c.mu.Lock()
	if c.epoch == epoch && c.max > 0 && time.Now().Before(started.Add(c.ttl)) {
		if len(c.entries) >= c.max {
			c.entries = make(map[string]entry)
		}
		c.entries[id] = entry{p, started.Add(c.ttl)}
	}
	c.mu.Unlock()
	return p, nil
}
