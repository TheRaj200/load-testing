package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"lastserver/internal/model"
)

func TestProductFillSurvivesUnrelatedWrite(t *testing.T) {
	c := NewProducts(10, time.Second)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	load := func(context.Context, string) (model.Product, error) {
		calls.Add(1)
		close(started)
		<-release
		return model.Product{ID: "p", Stock: 10}, nil
	}
	done := make(chan struct{})
	go func() { defer close(done); _, _ = c.Get(context.Background(), "p", load) }()
	<-started
	c.Invalidate("unrelated")
	close(release)
	<-done
	p, err := c.Get(context.Background(), "p", load)
	if err != nil || p.Stock != 10 || calls.Load() != 1 {
		t.Fatalf("product=%+v calls=%d err=%v", p, calls.Load(), err)
	}
}

func TestProductInvalidationDiscardsSlowFill(t *testing.T) {
	c := NewProducts(10, time.Second)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	load := func(context.Context, string) (model.Product, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return model.Product{ID: "p", Stock: 10}, nil
		}
		return model.Product{ID: "p", Stock: 9}, nil
	}
	result := make(chan model.Product, 1)
	go func() { p, _ := c.Get(context.Background(), "p", load); result <- p }()
	<-started
	c.Invalidate("p")
	close(release)
	p := <-result
	if p.Stock != 9 || calls.Load() != 2 {
		t.Fatalf("stale fill returned %+v after %d loads", p, calls.Load())
	}
}

func TestListCreationInvalidatesEmptyResult(t *testing.T) {
	c := NewLists(10, time.Second)
	var calls int
	load := func(_ context.Context, category string, page, limit int64) (model.ProductList, error) {
		calls++
		return model.ProductList{Items: []model.Product{}, Page: page, Limit: limit, Total: int64(calls - 1)}, nil
	}
	first, _ := c.Get(context.Background(), "new", 1, 20, load)
	c.InvalidateCategory("new")
	second, _ := c.Get(context.Background(), "new", 1, 20, load)
	if first.Total != 0 || second.Total != 1 || calls != 2 {
		t.Fatalf("before=%+v after=%+v calls=%d", first, second, calls)
	}
}

func TestListInvalidationDiscardsSlowFill(t *testing.T) {
	c := NewLists(10, time.Second)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	load := func(_ context.Context, category string, page, limit int64) (model.ProductList, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return model.ProductList{Items: []model.Product{}, Page: page, Limit: limit, Total: 0}, nil
		}
		return model.ProductList{Items: []model.Product{{ID: "new"}}, Page: page, Limit: limit, Total: 1}, nil
	}
	result := make(chan model.ProductList, 1)
	go func() { list, _ := c.Get(context.Background(), "new", 1, 20, load); result <- list }()
	<-started
	c.InvalidateCategory("new")
	close(release)
	list := <-result
	if list.Total != 1 || calls.Load() != 2 {
		t.Fatalf("stale list returned %+v after %d loads", list, calls.Load())
	}
}

func TestExpiredProductDoesNotSurviveDatabaseError(t *testing.T) {
	c := NewProducts(10, 10*time.Millisecond)
	_, _ = c.Get(context.Background(), "p", func(context.Context, string) (model.Product, error) { return model.Product{ID: "p", Stock: 1}, nil })
	time.Sleep(30 * time.Millisecond)
	problem := errors.New("database unavailable")
	_, err := c.Get(context.Background(), "p", func(context.Context, string) (model.Product, error) { return model.Product{}, problem })
	if !errors.Is(err, problem) {
		t.Fatalf("expired stock returned during database failure: %v", err)
	}
}
