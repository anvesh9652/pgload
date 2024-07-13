package pgdb

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type DB struct {
	schema     string
	dbConn     *sql.DB
	resetTable bool
}

func NewPostgresDB(url string, schema string, reset bool) (*DB, error) {
	conn, err := sql.Open("postgres", url)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open db connection")
	}
	if err = conn.Ping(); err != nil {
		return nil, errors.WithMessage(err, "failed to ping db")
	}

	fmt.Println("connected to database successfully")
	return &DB{
		schema:     schema,
		dbConn:     conn,
		resetTable: reset,
	}, nil
}

func (d *DB) EnsureTable(name string, tableSchema string) error {
	// to the check the schema exists
	createQuery := fmt.Sprintf("CREATE TABLE %s.%s %s", d.schema, name, tableSchema)
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
		if d.resetTable {
			_, err := d.dbConn.Exec(fmt.Sprintf("DROP TABLE %s.%s", d.schema, name))
			if err != nil {
				return err
			}
		}
	}
	_, err = d.dbConn.Exec(createQuery)
	return err
}

func (d *DB) SchemaName() string {
	return d.schema
}
