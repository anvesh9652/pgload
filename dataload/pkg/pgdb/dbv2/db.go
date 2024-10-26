package dbv2

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type DB struct {
	dbConn     *sqlx.DB
	schema     string
	resetTable bool
}

func NewPostgresDB(ctx context.Context, url, schema string, reset bool) (*DB, error) {
	dbConn, err := sqlx.ConnectContext(ctx, "pgx", url) // this also does the ping
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create db connection")
	}
	return &DB{dbConn, schema, reset}, nil
}

func (d *DB) GetRows(ctx context.Context, table string) error {
	q := fmt.Sprintf("select * from %s.%s limit 10", d.schema, table)
	rows, err := d.dbConn.QueryxContext(ctx, q)
	if err != nil {
		return err
	}

	for rows.Next() {
		row := map[string]any{}
		if err = rows.MapScan(row); err != nil {
			fmt.Println("here")
			return err
		}
		fmt.Println(row)
	}
	return nil
}

func (d *DB) EnsureTable(name string, tableSchema string) error {
	// Table names are being created with lowercase letters
	// even if we pass uppercase letters
	createQuery := fmt.Sprintf("CREATE TABLE %s.%s %s", d.schema, name, tableSchema)
	// to check if the schema exists
	_, err := d.dbConn.Exec(createQuery)
	if err == nil {
		return nil
	}

	errs := err.Error()
	if strings.Contains(errs, fmt.Sprintf(`schema "%s" does not exist`, d.schema)) {
		_, err := d.dbConn.Exec("CREATE SCHEMA " + d.schema)
		if err != nil {
			return err
		}
	}
	if strings.Contains(errs, fmt.Sprintf(`relation "%s" already exists`, name)) {
		if !d.resetTable {
			return nil
		}
		_, err := d.dbConn.Exec(fmt.Sprintf("DROP TABLE %s.%s", d.schema, name))
		if err != nil {
			return err
		}
	}
	_, err = d.dbConn.Exec(createQuery)
	return err
}

func (d *DB) DeleteTable(name string) error {
	_, err := d.dbConn.Exec(fmt.Sprintf("DROP TABLE %s.%s", d.schema, name))
	return err
}

func (d *DB) LoadIn(ctx context.Context, r io.Reader, copyCmd string) (int64, error) {
	conn, err := d.dbConn.Conn(ctx)
	if err != nil {
		return 0, err
	}
	var res pgconn.CommandTag
	err = conn.Raw(func(driverConn any) error {
		pgCon := driverConn.(*stdlib.Conn).Conn().PgConn()
		res, err = pgCon.CopyFrom(ctx, r, copyCmd)
		return err
	})
	return res.RowsAffected(), err
}

func (d *DB) Schema() string {
	return d.schema
}
