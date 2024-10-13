package pgdb

import (
	"context"
	"database/sql"
	"fmt"
	"io"
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

	fmt.Println("connected to database")
	return &DB{
		schema:     schema,
		dbConn:     conn,
		resetTable: reset,
	}, nil
}

func (d *DB) EnsureTable(name string, tableSchema string) error {
	// to check if the schema exists
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

func (d *DB) InsertRecords(name string, records []map[string]any, columns []string) error {
	if len(records) == 0 {
		return nil
	}
	query := fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES ", d.schema, name, strings.Join(columns, ", "))
	var vals []any
	params := 1
	for _, row := range records {
		var listParams []string
		for _, col := range columns {
			vals = append(vals, row[col])
			listParams = append(listParams, fmt.Sprintf("$%d", params))
			params += 1
		}
		query += fmt.Sprintf("(%s),", strings.Join(listParams, ","))
	}
	// remove (,) at the end
	query = query[:len(query)-1]
	stmt, err := d.dbConn.Prepare(query)
	if err != nil {
		return errors.WithMessage(err, "failed to preparte statement")
	}
	_, err = stmt.Exec(vals...)
	return err
}

func (d *DB) CopyFrom(name string, r io.Reader) error {
	ctx := context.Background()

	conn, err := d.dbConn.Conn(ctx)
	if err != nil {
		return err
	}

	fmt.Println("hi there")
	conn.Raw(func(driverConn any) error {
		fmt.Printf("%T\n", driverConn)
		// drc, err := driverConn.(*pg.Connector).Connect(ctx)
		// if err != nil{
		// 	return err
		// }
		// drc.
		// switch x := driverConn.(type) {
		// default:
		// 	fmt.Println(x)
		// }
		return nil
	})
	return nil
}

func (d *DB) SchemaName() string {
	return d.schema
}
