package config

import (
	"bufio"
	"os"
	"strings"
	"time"
)

type Config struct {
	Addr         string
	PostgresURL  string
	ValkeyURL    string
	RowsCacheTTL time.Duration
	S3Endpoint   string
	S3Bucket     string
	DevSyncKey   string
}

func FromEnv() Config {
	loadDotEnv(".env")

	return Config{
		Addr:         env("GONVEX_ADDR", ":8080"),
		PostgresURL:  env("DATABASE_URL", env("POSTGRES_URL", "")),
		ValkeyURL:    env("VALKEY_URL", env("REDIS_URL", "")),
		RowsCacheTTL: envDuration("GONVEX_ROWS_CACHE_TTL", 15*time.Second),
		S3Endpoint:   env("S3_ENDPOINT", ""),
		S3Bucket:     env("S3_BUCKET", ""),
		DevSyncKey:   env("GONVEX_DEV_SYNC_KEY", env("GONVEX_PROJECT_KEY", env("GONVEX_DEPLOY_KEY", ""))),
	}
}

func loadDotEnv(path string) {
	file, err := os.Open(path)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || os.Getenv(key) != "" {
			continue
		}
		os.Setenv(strings.TrimSpace(key), strings.TrimSpace(value))
	}
}

func env(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}
