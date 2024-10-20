package dbv2

import (
	"context"
	"io"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type DB struct {
	db *sqlx.DB
}

func NewPostgresDB(ctx context.Context, url string) (*DB, error) {
	dbConn, err := sqlx.Connect("pgx", url) // it also do the ping
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create db connection")
	}
	return &DB{db: dbConn}, nil
}

func (d *DB) LoadIn(ctx context.Context, r io.Reader, copyCmd string) error {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return err
	}

	err = conn.Raw(func(driverConn any) error {
		pgCon := driverConn.(*stdlib.Conn).Conn().PgConn()
		res, err := pgCon.CopyFrom(ctx, r, copyCmd)
		if err != nil {
			return err
		}
		_ = res
		return nil
	})
	return err
}
