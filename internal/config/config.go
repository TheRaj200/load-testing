package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Address           string
	DatabaseURL       string
	SchemaPath        string
	DBMaxConns        int32
	DBMinConns        int32
	DBConnectTimeout  time.Duration
	CacheMax          int
	CacheTTL          time.Duration
	ReadHeaderTimeout time.Duration
	IdleTimeout       time.Duration
}

func value(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func positiveInt(key string, fallback int) int {
	n, e := strconv.Atoi(value(key, strconv.Itoa(fallback)))
	if e != nil || n < 1 {
		return fallback
	}
	return n
}
func duration(key, fallback string) time.Duration {
	d, e := time.ParseDuration(value(key, fallback))
	if e != nil || d <= 0 {
		d, _ = time.ParseDuration(fallback)
	}
	return d
}

func Load() Config {
	ttl := duration("PRODUCT_CACHE_TTL", "1s")
	if ttl > 2*time.Second {
		ttl = 2 * time.Second
	}
	maxConns := positiveInt("DB_MAX_CONNS", 16)
	minConns := positiveInt("DB_MIN_CONNS", 2)
	if minConns > maxConns {
		minConns = maxConns
	}
	return Config{
		Address:     value("HOST", "0.0.0.0") + ":" + value("PORT", "8080"),
		DatabaseURL: value("DATABASE_URL", "postgres://competition:change-me@127.0.0.1:5432/competition?sslmode=disable"),
		SchemaPath:  value("SCHEMA_PATH", "migrations/001_init.sql"),
		DBMaxConns:  int32(maxConns), DBMinConns: int32(minConns),
		DBConnectTimeout: duration("DB_CONNECT_TIMEOUT", "5s"),
		CacheMax:         positiveInt("PRODUCT_CACHE_MAX", 20000), CacheTTL: ttl,
		ReadHeaderTimeout: duration("HTTP_READ_HEADER_TIMEOUT", "2s"),
		IdleTimeout:       duration("HTTP_IDLE_TIMEOUT", "60s"),
	}
}
