package data

import (
	"database/sql"
	"sync"

	"github.com/gonvex/gonvex/server/internal/dbpool"
	_ "github.com/jackc/pgx/v5/stdlib"
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
	dbpool.Configure(db)

	actual, loaded := dbPools.LoadOrStore(databaseURL, db)
	if loaded {
		_ = db.Close()
		return actual.(*sql.DB), nil
	}
	return db, nil
}
