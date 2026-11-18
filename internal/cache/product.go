package cache

import (
	"context"
	"sync"
	"time"

	"lastserver/internal/model"
)

type productEntry struct {
	product model.Product
	expires time.Time
}

type productFlight struct {
	done        chan struct{}
	product     model.Product
	err         error
	invalidated bool
}

// Products caches only successful reads. A missing ID is never cached because
// a concurrent product creation may make it valid immediately.
type Products struct {
	mu      sync.Mutex
	entries map[string]productEntry
	flights map[string]*productFlight
	max     int
	ttl     time.Duration
}

func NewProducts(max int, ttl time.Duration) *Products {
	if ttl <= 0 || ttl > 2*time.Second {
		ttl = time.Second
	}
	return &Products{entries: make(map[string]productEntry), flights: make(map[string]*productFlight), max: max, ttl: ttl}
}

func (c *Products) Invalidate(id string) {
	c.mu.Lock()
	delete(c.entries, id)
	if flight := c.flights[id]; flight != nil {
		flight.invalidated = true
		delete(c.flights, id)
	}
	c.mu.Unlock()
}

func (c *Products) Get(ctx context.Context, id string, load func(context.Context, string) (model.Product, error)) (model.Product, error) {
	for {
		c.mu.Lock()
		if entry, ok := c.entries[id]; ok && time.Now().Before(entry.expires) {
			c.mu.Unlock()
			return entry.product, nil
		}
		if flight := c.flights[id]; flight != nil {
			c.mu.Unlock()
			select {
			case <-flight.done:
				if flight.invalidated {
					continue
				}
				return flight.product, flight.err
			case <-ctx.Done():
				return model.Product{}, ctx.Err()
			}
		}
		if c.max <= 0 || len(c.flights) >= c.max {
			c.mu.Unlock()
			return load(ctx, id)
		}
		flight := &productFlight{done: make(chan struct{})}
		c.flights[id] = flight
		c.mu.Unlock()

		started := time.Now()
		product, err := load(ctx, id)
		c.mu.Lock()
		flight.product, flight.err = product, err
		invalidated := flight.invalidated
		if c.flights[id] == flight {
			delete(c.flights, id)
		}
		if !invalidated && err == nil && time.Now().Before(started.Add(c.ttl)) {
			if len(c.entries) >= c.max {
				c.entries = make(map[string]productEntry)
			}
			c.entries[id] = productEntry{product: product, expires: started.Add(c.ttl)}
		}
		close(flight.done)
		c.mu.Unlock()
		if invalidated {
			if err := ctx.Err(); err != nil {
				return model.Product{}, err
			}
			continue
		}
		return product, err
	}
}
