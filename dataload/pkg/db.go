package pkg

import (
	"database/sql"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type DB struct {
	schema string
	dbConn *sql.DB
}

func NewPostgresDB(url string, schema string) (*DB, error) {
	conn, err := sql.Open("postgres", url)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open db connection")
	}
	if err = conn.Ping(); err != nil {
		return nil, errors.WithMessage(err, "failed to ping db")
	}

	return &DB{
		schema: schema,
		dbConn: conn,
	}, nil
}

func (d *DB) SchemaName() string {
	return d.schema
}
