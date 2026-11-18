package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"lastserver/internal/model"
)

var (
	ErrProductNotFound  = errors.New("product not found")
	ErrProductExists    = errors.New("product exists")
	ErrAlreadyPurchased = errors.New("already purchased")
	ErrSoldOut          = errors.New("sold out")
	// The transaction may have committed even when the commit response was lost.
	ErrCommitUnknown = errors.New("order commit outcome unknown")
)

type Store struct{ db *pgxpool.Pool }

func New(db *pgxpool.Pool) *Store               { return &Store{db: db} }
func (s *Store) Ping(ctx context.Context) error { return s.db.Ping(ctx) }

func scanProduct(row pgx.Row) (model.Product, error) {
	var p model.Product
	err := row.Scan(&p.ID, &p.Name, &p.Category, &p.Price, &p.Stock)
	return p, err
}

func (s *Store) CreateProduct(ctx context.Context, p model.Product) (model.Product, error) {
	created, err := scanProduct(s.db.QueryRow(ctx, `INSERT INTO products(id,name,category,price,stock) VALUES($1,$2,$3,$4,$5) RETURNING id,name,category,price,stock`, p.ID, p.Name, p.Category, p.Price, p.Stock))
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" && pe.ConstraintName == "products_pkey" {
			return model.Product{}, ErrProductExists
		}
	}
	return created, err
}

func (s *Store) GetProduct(ctx context.Context, id string) (model.Product, error) {
	p, err := scanProduct(s.db.QueryRow(ctx, `SELECT id,name,category,price,stock FROM products WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrProductNotFound
	}
	return p, err
}

func (s *Store) ListProducts(ctx context.Context, category string, page, limit int64) (model.ProductList, error) {
	out := model.ProductList{Items: []model.Product{}, Page: page, Limit: limit}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(context.Background())
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM products WHERE category=$1`, category).Scan(&out.Total); err != nil {
		return out, err
	}
	if page-1 <= math.MaxInt64/limit && (page-1)*limit < out.Total {
		rows, err := tx.Query(ctx, `SELECT id,name,category,price,stock FROM products WHERE category=$1 ORDER BY price ASC,id COLLATE "C" ASC LIMIT $2 OFFSET $3`, category, limit, (page-1)*limit)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var p model.Product
			if err = rows.Scan(&p.ID, &p.Name, &p.Category, &p.Price, &p.Stock); err != nil {
				break
			}
			out.Items = append(out.Items, p)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return out, err
		}
	}
	return out, tx.Commit(ctx)
}

func orderID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "o_" + hex.EncodeToString(b[:]), nil
}

func (s *Store) CreateOrder(ctx context.Context, userID, productID string) (model.Order, error) {
	order := model.Order{UserID: userID, ProductID: productID}
	// Stock only decreases and products are never deleted. A committed zero is
	// therefore a safe rejection from the authoritative database, without a
	// transaction or a row lock on a heavily contested flash product.
	var stock int64
	err := s.db.QueryRow(ctx, `SELECT stock FROM products WHERE id=$1`, productID).Scan(&stock)
	if errors.Is(err, pgx.ErrNoRows) {
		return order, ErrProductNotFound
	}
	if err != nil {
		return order, err
	}
	if stock == 0 {
		return order, ErrSoldOut
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return order, err
	}
	defer tx.Rollback(context.Background())
	// Conditional UPDATE serializes the last-unit race. An INSERT failure
	// rolls this decrement back with the transaction.
	err = tx.QueryRow(ctx, `UPDATE products SET stock=stock-1 WHERE id=$1 AND stock>0 RETURNING stock`, productID).Scan(&stock)
	if errors.Is(err, pgx.ErrNoRows) {
		return order, ErrSoldOut
	}
	if err != nil {
		return order, err
	}
	order.OrderID, err = orderID()
	if err != nil {
		return order, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO orders(order_id,user_id,product_id) VALUES($1,$2,$3)`, order.OrderID, userID, productID)
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" && pe.ConstraintName == "orders_user_id_product_id_key" {
			return order, ErrAlreadyPurchased
		}
		return order, err
	}
	// Use a fresh context so a disconnected client cannot cancel a commit halfway.
	commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = tx.Commit(commitCtx); err != nil {
		return order, ErrCommitUnknown
	}
	return order, nil
}

func (s *Store) UserOrders(ctx context.Context, userID string) (model.UserOrders, error) {
	out := model.UserOrders{UserID: userID, Orders: []model.UserOrder{}}
	rows, err := s.db.Query(ctx, `SELECT order_id,product_id FROM orders WHERE user_id=$1`, userID)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var item model.UserOrder
		if err = rows.Scan(&item.OrderID, &item.ProductID); err != nil {
			break
		}
		out.Orders = append(out.Orders, item)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	return out, err
}
