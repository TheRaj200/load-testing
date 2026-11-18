// Run with --smoke locally. The default full profile is intended for the competition box.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type product struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Category string `json:"category"`
	Price    int64  `json:"price"`
	Stock    int64  `json:"stock"`
}
type order struct {
	OrderID   string `json:"orderId"`
	UserID    string `json:"userId"`
	ProductID string `json:"productId"`
}
type result struct {
	Status   int
	Body     []byte
	Duration time.Duration
	Err      error
}
type purchase struct{ userID, productID string }
type runner struct {
	base      string
	client    *http.Client
	run       string
	seq       atomic.Uint64
	wrong     atomic.Uint64
	transport atomic.Uint64
	mu        sync.Mutex
	accepted  map[string]map[string]bool
	uncertain map[string]int64
	initial   map[string]int64
	products  []product
	byID      map[string]product
	purchases []purchase
	users     []string
}

func brief(body []byte) string {
	if len(body) > 256 {
		body = body[:256]
	}
	return string(body)
}
func (x *runner) wrongAnswer(format string, args ...any) {
	n := x.wrong.Add(1)
	if n <= 20 {
		fmt.Printf("wrong_detail: "+format+"\n", args...)
	}
}
func (x *runner) failed(r result) bool {
	if r.Err != nil || r.Status >= 500 || r.Status == 429 {
		x.transport.Add(1)
		return true
	}
	return false
}

func (x *runner) request(ctx context.Context, method, path string, v any) result {
	var b []byte
	if v != nil {
		b, _ = json.Marshal(v)
	}
	req, e := http.NewRequestWithContext(ctx, method, x.base+path, bytes.NewReader(b))
	if e != nil {
		return result{Err: e}
	}
	if v != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	resp, e := x.client.Do(req)
	if e != nil {
		return result{Err: e, Duration: time.Since(start)}
	}
	defer resp.Body.Close()
	data, e := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return result{resp.StatusCode, data, time.Since(start), e}
}
func (x *runner) create(p product) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r := x.request(ctx, "POST", "/products", p)
	var got product
	if r.Err != nil || r.Status != 201 || json.Unmarshal(r.Body, &got) != nil || got != p {
		return fmt.Errorf("create %s: %d %s %v", p.ID, r.Status, r.Body, r.Err)
	}
	x.mu.Lock()
	x.initial[p.ID] = p.Stock
	x.products = append(x.products, p)
	x.byID[p.ID] = p
	x.mu.Unlock()
	return nil
}
func (x *runner) seed(n int) error {
	jobs := make(chan int)
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for k := 0; k < 50; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				p := product{fmt.Sprintf("%sp%05d", x.run, i), "Seed", fmt.Sprintf("%sc%02d", x.run, i%50), int64(i % 1000), 100000}
				if e := x.create(p); e != nil {
					errCh <- e
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(errCh)
	for e := range errCh {
		return e
	}
	return nil
}
func (x *runner) one(flash product, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	x.mu.Lock()
	products := x.products // Only appended between levels, after all workers finish.
	x.mu.Unlock()
	if len(products) == 0 {
		return
	}
	z := rng.Intn(100)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	switch {
	case z < 60:
		p := products[rng.Intn(len(products))]
		if rng.Intn(100) < 50 {
			p = products[rng.Intn(min(20, len(products)))]
		}
		id := p.ID
		missing := rng.Intn(50) == 0
		if missing {
			id = fmt.Sprintf("%smissing%d", x.run, seed)
		}
		r := x.request(ctx, "GET", "/products/"+id, nil)
		if x.failed(r) {
			return
		}
		if missing {
			var e map[string]string
			if r.Status != 404 || json.Unmarshal(r.Body, &e) != nil || e["error"] != "PRODUCT_NOT_FOUND" {
				x.wrongAnswer("missing product id=%s status=%d body=%s", id, r.Status, brief(r.Body))
			}
			return
		}
		var got product
		if r.Status != 200 || json.Unmarshal(r.Body, &got) != nil || got.ID != p.ID || got.Name != p.Name || got.Category != p.Category || got.Price != p.Price || got.Stock < 0 || got.Stock > p.Stock {
			x.wrongAnswer("product id=%s status=%d body=%s", id, r.Status, brief(r.Body))
		}
	case z < 75:
		p := products[rng.Intn(len(products))]
		page, limit := 1, []int{10, 20, 50}[rng.Intn(3)]
		if rng.Intn(10) == 0 {
			page = 1 + rng.Intn(10)
		}
		r := x.request(ctx, "GET", fmt.Sprintf("/products?category=%s&page=%d&limit=%d", p.Category, page, limit), nil)
		if x.failed(r) {
			return
		}
		var got struct {
			Items []product `json:"items"`
			Page  int       `json:"page"`
			Limit int       `json:"limit"`
			Total int       `json:"total"`
		}
		if r.Status != 200 || json.Unmarshal(r.Body, &got) != nil || got.Items == nil || got.Page != page || got.Limit != limit || got.Total < 1 {
			x.wrongAnswer("list category=%s page=%d limit=%d status=%d body=%s", p.Category, page, limit, r.Status, brief(r.Body))
			return
		}
		for i, item := range got.Items {
			if item.Category != p.Category || item.Stock < 0 || item.Stock > 100000 || (i > 0 && (got.Items[i-1].Price > item.Price || (got.Items[i-1].Price == item.Price && got.Items[i-1].ID >= item.ID))) {
				x.wrongAnswer("list item category=%s page=%d item=%d body=%s", p.Category, page, i, brief(r.Body))
				return
			}
		}
	case z < 95:
		p := products[rng.Intn(len(products))]
		if flash.ID != "" && rng.Intn(10) < 7 {
			p = flash
		}
		user := fmt.Sprintf("%su%d", x.run, x.seq.Add(1))
		if rng.Intn(100) < 3 {
			x.mu.Lock()
			if len(x.purchases) > 0 {
				previous := x.purchases[rng.Intn(len(x.purchases))]
				user = previous.userID
				p = x.byID[previous.productID]
			}
			x.mu.Unlock()
		}
		payload := map[string]string{"userId": user, "productId": p.ID}
		responses := []result{}
		if rng.Intn(100) < 2 {
			ch := make(chan result, 2)
			go func() { ch <- x.request(ctx, "POST", "/orders", payload) }()
			go func() { ch <- x.request(ctx, "POST", "/orders", payload) }()
			responses = append(responses, <-ch, <-ch)
		} else {
			responses = append(responses, x.request(ctx, "POST", "/orders", payload))
		}
		for _, r := range responses {
			x.checkOrder(r, p, user)
		}
	case z < 100:
		user := fmt.Sprintf("%sunknown%d", x.run, seed)
		x.mu.Lock()
		if len(x.users) > 0 && rng.Intn(10) < 8 {
			user = x.users[rng.Intn(len(x.users))]
		}
		x.mu.Unlock()
		r := x.request(ctx, "GET", "/users/"+user+"/orders", nil)
		if x.failed(r) {
			return
		}
		var got struct {
			UserID string  `json:"userId"`
			Orders []order `json:"orders"`
		}
		if r.Status != 200 || json.Unmarshal(r.Body, &got) != nil || got.UserID != user || got.Orders == nil {
			x.wrongAnswer("user orders user=%s status=%d body=%s", user, r.Status, brief(r.Body))
		}
	}
}
func (x *runner) checkOrder(r result, p product, user string) {
	if x.failed(r) {
		// A timed-out order can still commit after its response is lost. Keep it
		// in the audit's upper bound; a transport error is not proof of rollback.
		if r.Err != nil {
			x.mu.Lock()
			x.uncertain[p.ID]++
			x.mu.Unlock()
		}
		return
	}
	if r.Status == 201 {
		var o order
		if json.Unmarshal(r.Body, &o) != nil || o.OrderID == "" || o.UserID != user || o.ProductID != p.ID {
			x.wrongAnswer("order product=%s user=%s status=%d body=%s", p.ID, user, r.Status, brief(r.Body))
			return
		}
		x.mu.Lock()
		if x.accepted[p.ID] == nil {
			x.accepted[p.ID] = map[string]bool{}
		}
		if x.accepted[p.ID][user] {
			x.wrongAnswer("duplicate success product=%s user=%s", p.ID, user)
		}
		x.accepted[p.ID][user] = true
		x.purchases = append(x.purchases, purchase{user, p.ID})
		x.users = append(x.users, user)
		x.mu.Unlock()
	} else if r.Status == 409 {
		var e map[string]string
		if json.Unmarshal(r.Body, &e) != nil || (e["error"] != "SOLD_OUT" && e["error"] != "ALREADY_PURCHASED") || p.Name != "Flash" && e["error"] == "SOLD_OUT" {
			x.wrongAnswer("order conflict product=%s user=%s kind=%s body=%s", p.ID, user, p.Name, brief(r.Body))
		}
	} else {
		x.wrongAnswer("order status product=%s user=%s status=%d body=%s", p.ID, user, r.Status, brief(r.Body))
	}
}
func (x *runner) level(rate int, d time.Duration, flash product) {
	var wg sync.WaitGroup
	slots := make(chan struct{}, rate*2)
	start := time.Now()
	oldWrong, oldTransport := x.wrong.Load(), x.transport.Load()
	lat := make([]time.Duration, 0, rate*int(d/time.Second))
	var lm sync.Mutex
	for i := 0; i < rate*int(d/time.Second); i++ {
		target := start.Add(time.Duration(i) * time.Second / time.Duration(rate))
		if delay := time.Until(target); delay > 0 {
			time.Sleep(delay)
		}
		select {
		case slots <- struct{}{}:
			wg.Add(1)
			n := x.seq.Add(1)
			go func() {
				defer wg.Done()
				begin := time.Now()
				x.one(flash, int64(n))
				lm.Lock()
				lat = append(lat, time.Since(begin))
				lm.Unlock()
				<-slots
			}()
		default:
			x.transport.Add(1)
		}
	}
	wg.Wait()
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p99 := time.Duration(0)
	if len(lat) > 0 {
		p99 = lat[min(len(lat)-1, len(lat)*99/100)]
	}
	fmt.Printf("rate=%d sent=%d p99=%s transport=%d wrong=%d (same-host measurement)\n", rate, len(lat), p99, x.transport.Load()-oldTransport, x.wrong.Load()-oldWrong)
}
func (x *runner) audit() error {
	time.Sleep(3 * time.Second)
	x.mu.Lock()
	ids := make([]string, 0, len(x.initial))
	for id := range x.initial {
		ids = append(ids, id)
	}
	x.mu.Unlock()
	for _, id := range ids {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		r := x.request(ctx, "GET", "/products/"+id, nil)
		cancel()
		var p product
		if r.Err != nil || r.Status != 200 || json.Unmarshal(r.Body, &p) != nil {
			return fmt.Errorf("audit read %s", id)
		}
		x.mu.Lock()
		accepted := int64(len(x.accepted[id]))
		maxAccepted := accepted + x.uncertain[id]
		x.mu.Unlock()
		if p.Stock > x.initial[id]-accepted || p.Stock < x.initial[id]-maxAccepted {
			return fmt.Errorf("audit %s stock=%d accepted=%d uncertain=%d", id, p.Stock, accepted, maxAccepted-accepted)
		}
	}
	return nil
}
func main() {
	base := flag.String("base", "http://127.0.0.1:8080", "target URL")
	smoke := flag.Bool("smoke", false, "small local run")
	maxRate := flag.Int("max-rate", 50000, "highest full-profile rate to run")
	flag.Parse()
	run := fmt.Sprintf("r%d", time.Now().UnixNano())
	x := &runner{base: *base, run: run, client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{MaxIdleConns: 5000, MaxIdleConnsPerHost: 5000, IdleConnTimeout: 60 * time.Second}}, accepted: map[string]map[string]bool{}, uncertain: map[string]int64{}, initial: map[string]int64{}, byID: map[string]product{}}
	seed := 10000
	if *smoke {
		seed = 100
	}
	if e := x.seed(seed); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if *smoke {
		x.level(50, 5*time.Second, product{})
	} else {
		x.level(200, 60*time.Second, product{})
		for i, rate := range []int{500, 1000, 1500, 2000, 3000, 4000, 5000, 6500, 8000, 10000, 12500, 15000, 20000, 25000, 30000, 40000, 50000} {
			if rate > *maxRate {
				break
			}
			p := product{fmt.Sprintf("%sflash%d", run, i), "Flash", fmt.Sprintf("%sf%d", run, i), 2999, int64(50 + i%151)}
			if e := x.create(p); e != nil {
				fmt.Fprintln(os.Stderr, e)
				os.Exit(1)
			}
			x.level(rate, 30*time.Second, p)
		}
	}
	if e := x.audit(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
