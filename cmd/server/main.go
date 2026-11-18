package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"lastserver/internal/api"
	"lastserver/internal/cache"
	"lastserver/internal/config"
	"lastserver/internal/store"
)

func main() {
	settings := config.Load()
	poolConfig, err := pgxpool.ParseConfig(settings.DatabaseURL)
	if err != nil {
		log.Fatal("invalid DATABASE_URL")
	}
	poolConfig.MaxConns = settings.DBMaxConns
	poolConfig.MinConns = settings.DBMinConns
	poolConfig.ConnConfig.ConnectTimeout = settings.DBConnectTimeout
	db, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatal("database pool: ", err)
	}
	defer db.Close()
	if err = db.Ping(context.Background()); err != nil {
		log.Fatal("database ping: ", err)
	}
	schema, err := os.ReadFile(settings.SchemaPath)
	if err != nil {
		log.Fatal("schema: ", err)
	}
	if _, err = db.Exec(context.Background(), string(schema)); err != nil {
		log.Fatal("schema initialization: ", err)
	}
	handler := api.New(store.New(db), cache.NewProducts(settings.CacheMax, settings.CacheTTL), cache.NewLists(settings.ListCacheMax, settings.ListCacheTTL))
	server := &http.Server{Addr: settings.Address, Handler: handler, ReadHeaderTimeout: settings.ReadHeaderTimeout, IdleTimeout: settings.IdleTimeout}
	log.Printf("listening on %s", server.Addr)
	if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
