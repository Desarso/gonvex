package dbpool

import (
	"database/sql"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultMaxOpen = 0
	defaultMaxIdle = 100
	defaultIdleTTL = 5 * time.Minute
)

// Limits describes the application-side physical PostgreSQL connection pool.
// A MaxOpen value of zero is database/sql's documented unlimited setting.
type Limits struct {
	MaxOpen int
	MaxIdle int
}

// LimitsFromEnvironment returns a deliberately elastic pool: requests may open
// more than the warm target under load, while only MaxIdle connections remain
// ready after traffic subsides.
func LimitsFromEnvironment() Limits {
	limits := Limits{
		MaxOpen: environmentInt("GONVEX_DB_MAX_OPEN_CONNS", defaultMaxOpen),
		MaxIdle: environmentInt("GONVEX_DB_MAX_IDLE_CONNS", defaultMaxIdle),
	}
	if limits.MaxOpen > 0 && limits.MaxIdle > limits.MaxOpen {
		limits.MaxIdle = limits.MaxOpen
	}
	return limits
}

func Configure(db *sql.DB) {
	if db == nil {
		return
	}
	limits := LimitsFromEnvironment()
	db.SetMaxOpenConns(limits.MaxOpen)
	db.SetMaxIdleConns(limits.MaxIdle)
	db.SetConnMaxIdleTime(defaultIdleTTL)
}

func environmentInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}
