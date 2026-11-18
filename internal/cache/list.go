package cache

import (
	"context"
	"sync"
	"time"

	"lastserver/internal/model"
)

type listKey struct {
	category    string
	page, limit int64
}
type listEntry struct {
	result  model.ProductList
	expires time.Time
}
type listFlight struct {
	done        chan struct{}
	result      model.ProductList
	err         error
	invalidated bool
}

// Lists keeps stock for a fixed short lifetime. CreateProduct clears entries
// before its 201 response, so category membership and totals do not linger.
type Lists struct {
	mu      sync.Mutex
	entries map[listKey]listEntry
	flights map[listKey]*listFlight
	max     int
	ttl     time.Duration
}

func NewLists(max int, ttl time.Duration) *Lists {
	if ttl <= 0 || ttl > 2*time.Second {
		ttl = time.Second
	}
	return &Lists{entries: make(map[listKey]listEntry), flights: make(map[listKey]*listFlight), max: max, ttl: ttl}
}

func (c *Lists) InvalidateCategory(_ string) {
	c.mu.Lock()
	c.entries = make(map[listKey]listEntry)
	for key, flight := range c.flights {
		flight.invalidated = true
		delete(c.flights, key)
	}
	c.mu.Unlock()
}

func (c *Lists) Get(ctx context.Context, category string, page, limit int64, load func(context.Context, string, int64, int64) (model.ProductList, error)) (model.ProductList, error) {
	key := listKey{category: category, page: page, limit: limit}
	for {
		c.mu.Lock()
		if entry, ok := c.entries[key]; ok && time.Now().Before(entry.expires) {
			c.mu.Unlock()
			return entry.result, nil
		}
		if flight := c.flights[key]; flight != nil {
			c.mu.Unlock()
			select {
			case <-flight.done:
				if flight.invalidated {
					continue
				}
				return flight.result, flight.err
			case <-ctx.Done():
				return model.ProductList{}, ctx.Err()
			}
		}
		if c.max <= 0 || len(c.flights) >= c.max {
			c.mu.Unlock()
			return load(ctx, category, page, limit)
		}
		flight := &listFlight{done: make(chan struct{})}
		c.flights[key] = flight
		c.mu.Unlock()

		started := time.Now()
		result, err := load(ctx, category, page, limit)
		c.mu.Lock()
		flight.result, flight.err = result, err
		invalidated := flight.invalidated
		if c.flights[key] == flight {
			delete(c.flights, key)
		}
		if !invalidated && err == nil && time.Now().Before(started.Add(c.ttl)) {
			if len(c.entries) >= c.max {
				c.entries = make(map[listKey]listEntry)
			}
			c.entries[key] = listEntry{result: result, expires: started.Add(c.ttl)}
		}
		close(flight.done)
		c.mu.Unlock()
		if invalidated {
			if err := ctx.Err(); err != nil {
				return model.ProductList{}, err
			}
			continue
		}
		return result, err
	}
}
