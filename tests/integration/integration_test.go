package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"lastserver/internal/api"
	"lastserver/internal/cache"
	"lastserver/internal/store"
)

type fixture struct {
	server *api.Server
	db     *pgxpool.Pool
}

func testApp(t *testing.T) (*fixture, func()) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TEST_DATABASE_URL to a disposable PostgreSQL database")
	}
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		t.Fatal(e)
	}
	name := "competition_test_" + strings.ReplaceAll(fmt.Sprint(time.Now().UnixNano()), "-", "")
	admin, e := pgxpool.NewWithConfig(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(context.Background(), "CREATE SCHEMA "+name); e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = name
	pool, e := pgxpool.NewWithConfig(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	schema, e := os.ReadFile("../../migrations/001_init.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(context.Background(), string(schema)); e != nil {
		t.Fatal(e)
	}
	a := &fixture{server: api.New(store.New(pool), cache.NewProducts(1000, time.Second)), db: pool}
	return a, func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+name+" CASCADE")
		admin.Close()
	}
}
func call(a *fixture, method, path, body string) (int, map[string]any) {
	r := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	a.server.ServeHTTP(w, r)
	var v map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &v)
	return w.Code, v
}
func require(t *testing.T, a *fixture, method, path, body string, status int, code string) map[string]any {
	t.Helper()
	s, v := call(a, method, path, body)
	if s != status || (code != "" && v["error"] != code) {
		t.Fatalf("%s %s %s: %d %v; want %d %s", method, path, body, s, v, status, code)
	}
	return v
}
func TestContract(t *testing.T) {
	a, done := testApp(t)
	defer done()
	require(t, a, "GET", "/health", "", 200, "")
	product := `{"id":"p","name":"A","category":"c","price":0,"stock":2,"extra":true}`
	for _, b := range []string{`{}`, `{"id":null}`, `{"id":"","name":"A","category":"c","price":0,"stock":2}`, `{"id":"p","name":"A","category":"c","price":null,"stock":2}`, `{"id":"p","name":"A","category":"c","price":"0","stock":2}`, `{"id":"p","name":"A","category":"c","price":1.5,"stock":2}`, `{"id":"p","name":"A","category":"c","price":0,"stock":-1}`} {
		require(t, a, "POST", "/products", b, 400, "VALIDATION_ERROR")
	}
	v := require(t, a, "POST", "/products", product, 201, "")
	if v["price"] != float64(0) || v["stock"] != float64(2) {
		t.Fatal(v)
	}
	require(t, a, "POST", "/products", `{"id":"p","name":"changed","category":"other","price":99,"stock":99}`, 409, "PRODUCT_EXISTS")
	v = require(t, a, "GET", "/products/p", "", 200, "")
	if v["name"] != "A" || v["stock"] != float64(2) {
		t.Fatal(v)
	}
	require(t, a, "GET", "/products/missing", "", 404, "PRODUCT_NOT_FOUND")
	for _, path := range []string{"/products", "/products?category=", "/products?category=c&page=0", "/products?category=c&page=", "/products?category=c&limit=51", "/products?category=c&limit=1.5"} {
		require(t, a, "GET", path, "", 400, "VALIDATION_ERROR")
	}
	require(t, a, "POST", "/products", `{"id":"a","name":"Tie","category":"c","price":0,"stock":0}`, 201, "")
	v = require(t, a, "GET", "/products?category=c&limit=1", "", 200, "")
	if v["total"] != float64(2) || v["items"].([]any)[0].(map[string]any)["id"] != "a" {
		t.Fatal(v)
	}
	v = require(t, a, "GET", "/products?category=c&page=9223372036854775807", "", 200, "")
	if len(v["items"].([]any)) != 0 || v["total"] != float64(2) {
		t.Fatal(v)
	}
	v = require(t, a, "GET", "/products?category=absent", "", 200, "")
	if len(v["items"].([]any)) != 0 || v["total"] != float64(0) {
		t.Fatal(v)
	}
	for _, b := range []string{`{}`, `{"userId":null,"productId":"p"}`, `{"userId":"","productId":"p"}`, `{"userId":1,"productId":"p"}`} {
		require(t, a, "POST", "/orders", b, 400, "VALIDATION_ERROR")
	}
	require(t, a, "POST", "/orders", `{"userId":"u","productId":"missing"}`, 404, "PRODUCT_NOT_FOUND")
	require(t, a, "POST", "/orders", `{"userId":"u","productId":"a"}`, 409, "SOLD_OUT")
	require(t, a, "POST", "/orders", `{"userId":"u","productId":"p"}`, 201, "")
	require(t, a, "POST", "/orders", `{"userId":"u","productId":"p"}`, 409, "ALREADY_PURCHASED")
	v = require(t, a, "GET", "/users/u/orders", "", 200, "")
	if len(v["orders"].([]any)) != 1 {
		t.Fatal(v)
	}
	v = require(t, a, "GET", "/users/unknown/orders", "", 200, "")
	if len(v["orders"].([]any)) != 0 {
		t.Fatal(v)
	}
	// A new pool and handler must see committed data after replacing the process state.
	newPool, e := pgxpool.NewWithConfig(context.Background(), a.db.Config().Copy())
	if e != nil {
		t.Fatal(e)
	}
	defer newPool.Close()
	b := &fixture{server: api.New(store.New(newPool), cache.NewProducts(1000, time.Second)), db: newPool}
	v = require(t, b, "GET", "/products/p", "", 200, "")
	if v["stock"] != float64(1) {
		t.Fatal(v)
	}
}
func TestConcurrentSales(t *testing.T) {
	a, done := testApp(t)
	defer done()
	for _, tc := range []struct {
		name          string
		stock, buyers int
		same          bool
		want          int
	}{{"last", 1, 50, false, 1}, {"double", 1000, 20, true, 1}, {"bulk", 100, 150, false, 100}} {
		require(t, a, "POST", "/products", fmt.Sprintf(`{"id":%q,"name":"Race","category":"r","price":1,"stock":%d}`, tc.name, tc.stock), 201, "")
		var wg sync.WaitGroup
		results := make(chan int, tc.buyers)
		for i := 0; i < tc.buyers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				user := fmt.Sprintf("u%d", i)
				if tc.same {
					user = "same"
				}
				s, _ := call(a, "POST", "/orders", fmt.Sprintf(`{"userId":%q,"productId":%q}`, user, tc.name))
				results <- s
			}(i)
		}
		wg.Wait()
		close(results)
		wins := 0
		for s := range results {
			if s == 201 {
				wins++
			} else if s != 409 {
				t.Fatalf("%s unexpected %d", tc.name, s)
			}
		}
		if wins != tc.want {
			t.Fatalf("%s: %d wins, want %d", tc.name, wins, tc.want)
		}
		v := require(t, a, "GET", "/products/"+tc.name, "", 200, "")
		if v["stock"] != float64(tc.stock-tc.want) {
			t.Fatal(v)
		}
		var count int
		if e := a.db.QueryRow(context.Background(), `SELECT count(*) FROM orders WHERE product_id=$1`, tc.name).Scan(&count); e != nil || count != tc.want {
			t.Fatalf("%s orders %d: %v", tc.name, count, e)
		}
	}
}
