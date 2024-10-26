package jsonloader

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
	"github.com/anvesh9652/side-projects/shared"
	"github.com/sourcegraph/conc/pool"
)

type row map[string]any

type JsonLoader struct {
	maxConcurrency int
	lookUpSize     int

	typeSetting string

	filesList []string

	db *dbv2.DB
}

func New(files []string, db *dbv2.DB, concurrency, lookUp int, t string) *JsonLoader {
	return &JsonLoader{
		maxConcurrency: concurrency,
		typeSetting:    t,
		lookUpSize:     lookUp,
		db:             db,
		filesList:      files,
	}
}

func (j *JsonLoader) Run(ctx context.Context) error {
	err := shared.RunInParallel(j.maxConcurrency, j.filesList, func(file string) error {
		var err error

		name := shared.GetTableName(file)
		defer func() {
			if err != nil {
				_ = j.db.DeleteTable(name)
			}
		}()
		colsTypes, cols, err := j.findTypesAndGetCols(file)
		if err != nil {
			return err
		}

		err = j.db.EnsureTable(name, fmt.Sprintf("(%s)", strings.Join(colsTypes, ", ")))
		if err != nil {
			return err
		}

		pr, pw := io.Pipe()

		p := pool.New().WithErrors().WithFirstError()
		p.Go(func() error {
			defer pw.Close()
			return convertJsonlToCSV(pw, file, cols)
		})

		copyCmd := fmt.Sprintf(`COPY %s.%s(%s) FROM STDIN with DELIMITER %s %s`,
			j.db.Schema(), name, strings.Join(cols, ", "), "','", "CSV",
		)

		rowsInserted, err := j.db.LoadIn(ctx, pr, copyCmd)
		if err != nil {
			return err
		}
		if err = p.Wait(); err != nil {
			return err
		}
		fmt.Println(rowsInserted)
		return nil
	})
	return err
}

func convertJsonlToCSV(w io.Writer, file string, cols []string) error {

	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	cw := csv.NewWriter(w)
	defer cw.Flush()

	dec := json.NewDecoder(f)
	for dec.More() {
		var r row
		if err = dec.Decode(&r); err != nil {
			return err
		}
		var csvRow = make([]string, len(cols))
		for i, header := range cols {
			csvRow[i] = toString(r[header])
		}
		if err = cw.Write(csvRow); err != nil {
			return err
		}
	}
	return nil
}

func toString(val any) string {
	return ""
}

func (j *JsonLoader) findTypesAndGetCols(file string) ([]string, []string, error) {
	var (
		rows    []row
		headers = make(map[string]struct{})

		types []string
	)
	f, err := os.Open(file)
	if err != nil {
		return nil, nil, err
	}

	dec := json.NewDecoder(f)
	for i := 0; i < j.lookUpSize && dec.More(); i++ {
		var r row
		if err = dec.Decode(&r); err != nil {
			return nil, nil, err
		}
		// we running this for all rows just to ensure that we get all the keys,
		// sometime a row might have less keys compared to the previous row
		for col := range r {
			headers[col] = struct{}{}
		}
		rows = append(rows, r)
	}

	var colsList []string
	for header := range headers {
		colsList = append(colsList, header)
		if j.typeSetting == shared.AllText {
			types = append(types, header+", TEXT")
			continue
		}

		uniqueTypes := map[string]int{}
		for _, row := range rows {
			val := row[header]
			uniqueTypes[getType(val)]++
		}

		types = append(types, header+", "+maxRecordedType(uniqueTypes))
	}
	return types, colsList, nil
}

func getType(val any) string {
	switch val.(type) {
	case float64, int:
		return "DECIMAL"
	case []any, any:
		return "OBJECT"
	default:
		return "TEXT"
	}
}

func maxRecordedType(map[string]int) string {
	// todo: do this later
	return "TEXT"
}
