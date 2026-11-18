package api

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"lastserver/internal/cache"
	"lastserver/internal/model"
	"lastserver/internal/store"
)

type Server struct {
	store *store.Store
	cache *cache.Products
	lists *cache.Lists
}

func New(s *store.Store, c *cache.Products, lists *cache.Lists) *Server {
	return &Server{store: s, cache: c, lists: lists}
}

func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func apiError(w http.ResponseWriter, status int, code string) {
	respond(w, status, map[string]string{"error": code})
}

func (a *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	switch {
	case path == "health" && r.Method == http.MethodGet:
		a.health(w, r)
	case path == "products" && r.Method == http.MethodPost:
		a.createProduct(w, r)
	case path == "products" && r.Method == http.MethodGet:
		a.listProducts(w, r)
	case strings.HasPrefix(path, "products/") && strings.Count(path, "/") == 1 && r.Method == http.MethodGet:
		a.getProduct(w, r, strings.TrimPrefix(path, "products/"))
	case path == "orders" && r.Method == http.MethodPost:
		a.createOrder(w, r)
	case strings.HasPrefix(path, "users/") && strings.HasSuffix(path, "/orders") && strings.Count(path, "/") == 2 && r.Method == http.MethodGet:
		a.userOrders(w, r, strings.TrimSuffix(strings.TrimPrefix(path, "users/"), "/orders"))
	default:
		apiError(w, 404, "VALIDATION_ERROR")
	}
}

func (a *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if a.store.Ping(ctx) != nil {
		respond(w, 503, map[string]string{"status": "unavailable"})
		return
	}
	respond(w, 200, map[string]string{"status": "ok"})
}

func (a *Server) createProduct(w http.ResponseWriter, r *http.Request) {
	fields, err := readObject(r)
	if err != nil {
		apiError(w, 400, "VALIDATION_ERROR")
		return
	}
	id, ok1 := stringField(fields, "id", 64)
	name, ok2 := stringField(fields, "name", 200)
	category, ok3 := stringField(fields, "category", 64)
	price, ok4 := intField(fields, "price")
	stock, ok5 := intField(fields, "stock")
	if !(ok1 && ok2 && ok3 && ok4 && ok5) {
		apiError(w, 400, "VALIDATION_ERROR")
		return
	}
	p, err := a.store.CreateProduct(r.Context(), model.Product{ID: id, Name: name, Category: category, Price: price, Stock: stock})
	if errors.Is(err, store.ErrProductExists) {
		apiError(w, 409, "PRODUCT_EXISTS")
		return
	}
	if err != nil {
		apiError(w, 500, "INTERNAL_ERROR")
		return
	}
	a.cache.Invalidate(id)
	a.lists.InvalidateCategory(category)
	respond(w, 201, p)
}

func (a *Server) getProduct(w http.ResponseWriter, r *http.Request, id string) {
	if !validString(id, 64) {
		apiError(w, 404, "PRODUCT_NOT_FOUND")
		return
	}
	p, err := a.cache.Get(r.Context(), id, a.store.GetProduct)
	if errors.Is(err, store.ErrProductNotFound) {
		apiError(w, 404, "PRODUCT_NOT_FOUND")
		return
	}
	if err != nil {
		apiError(w, 500, "INTERNAL_ERROR")
		return
	}
	respond(w, 200, p)
}

func (a *Server) listProducts(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	categories, ok := query["category"]
	if !ok || len(categories) != 1 || categories[0] == "" {
		apiError(w, 400, "VALIDATION_ERROR")
		return
	}
	page, limit := int64(1), int64(20)
	if values, exists := query["page"]; exists {
		if len(values) != 1 {
			apiError(w, 400, "VALIDATION_ERROR")
			return
		}
		page, ok = positive(values[0], math.MaxInt64)
		if !ok {
			apiError(w, 400, "VALIDATION_ERROR")
			return
		}
	}
	if values, exists := query["limit"]; exists {
		if len(values) != 1 {
			apiError(w, 400, "VALIDATION_ERROR")
			return
		}
		limit, ok = positive(values[0], 50)
		if !ok {
			apiError(w, 400, "VALIDATION_ERROR")
			return
		}
	}
	out, err := a.lists.Get(r.Context(), categories[0], page, limit, a.store.ListProducts)
	if err != nil {
		apiError(w, 500, "INTERNAL_ERROR")
		return
	}
	respond(w, 200, out)
}

func (a *Server) createOrder(w http.ResponseWriter, r *http.Request) {
	fields, err := readObject(r)
	if err != nil {
		apiError(w, 400, "VALIDATION_ERROR")
		return
	}
	user, ok1 := stringField(fields, "userId", 64)
	product, ok2 := stringField(fields, "productId", 64)
	if !ok1 || !ok2 {
		apiError(w, 400, "VALIDATION_ERROR")
		return
	}
	order, err := a.store.CreateOrder(r.Context(), user, product)
	switch {
	case errors.Is(err, store.ErrProductNotFound):
		apiError(w, 404, "PRODUCT_NOT_FOUND")
	case errors.Is(err, store.ErrAlreadyPurchased):
		apiError(w, 409, "ALREADY_PURCHASED")
	case errors.Is(err, store.ErrSoldOut):
		apiError(w, 409, "SOLD_OUT")
	case errors.Is(err, store.ErrCommitUnknown):
		closeUncertain(w)
	case err != nil:
		apiError(w, 500, "INTERNAL_ERROR")
	default:
		a.cache.DecrementStock(product)
		respond(w, 201, order)
	}
}

func closeUncertain(w http.ResponseWriter) {
	if h, ok := w.(http.Hijacker); ok {
		conn, _, err := h.Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}
}

func (a *Server) userOrders(w http.ResponseWriter, r *http.Request, user string) {
	out, err := a.store.UserOrders(r.Context(), user)
	if err != nil {
		apiError(w, 500, "INTERNAL_ERROR")
		return
	}
	respond(w, 200, out)
}
