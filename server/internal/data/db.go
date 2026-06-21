package data

import (
	"database/sql"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultMaxOpenConns = 2
	defaultMaxIdleConns = 1
	defaultConnIdleTime = 5 * time.Minute
)

var dbPools sync.Map

func openDB(databaseURL string) (*sql.DB, error) {
	if cached, ok := dbPools.Load(databaseURL); ok {
		return cached.(*sql.DB), nil
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(defaultMaxOpenConns)
	db.SetMaxIdleConns(defaultMaxIdleConns)
	db.SetConnMaxIdleTime(defaultConnIdleTime)

	actual, loaded := dbPools.LoadOrStore(databaseURL, db)
	if loaded {
		_ = db.Close()
		return actual.(*sql.DB), nil
	}
	return db, nil
}
