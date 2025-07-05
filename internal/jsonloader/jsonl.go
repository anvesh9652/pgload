package jsonloader

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	csv2 "github.com/anvesh9652/pgload/internal/csvloader/v2"
	"github.com/anvesh9652/pgload/internal/pgdb/dbv2"
	"github.com/anvesh9652/pgload/pkg/shared"
	"github.com/anvesh9652/pgload/pkg/shared/reader"
	"github.com/sourcegraph/conc/pool"

	"github.com/anvesh9652/concurrent-line-processor/examples/codes"
)

const (
	batchSize = 3000 // 500
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

func (j *JsonLoader) Run(ctx context.Context) (string, error) {
	var totalRowsInserted, failed int64
	start := time.Now()

	err := shared.RunInParallel(j.maxConcurrency, j.filesList, func(file string) error {
		var err error

		name := shared.GetTableName(file)
		defer func() {
			if err != nil {
				atomic.AddInt64(&failed, int64(1))
				_ = j.db.DeleteTable(name)
			}
		}()
		colsTypes, cols, err := j.findTypesAndGetCols(file)
		if err != nil {
			printError(file, name, err)
			return err
		}

		// Ensure the table exists or create it if necessary.
		err = j.db.EnsureTable(name, fmt.Sprintf("(%s)", strings.Join(colsTypes, ", ")))
		if err != nil {
			printError(file, name, err)
			return err
		}

		pr, pw := io.Pipe()

		p := pool.New().WithErrors().WithFirstError()
		p.Go(func() error {
			defer pw.Close()
			return convertJsonlToCSV2(pw, file, cols)
		})

		rowsInserted, err := csv2.LoadCSV(ctx, pr, name, j.db)
		if err != nil {
			printError(file, name, err)
			return err
		}
		if err = p.Wait(); err != nil {
			printError(file, name, err)
			return err
		}
		atomic.AddInt64(&totalRowsInserted, rowsInserted)
		fmt.Printf("status=SUCCESS rows_inserted=%s file_size=%s file=%s\n",
			shared.FormatNumber(rowsInserted), shared.GetFileSize(file), file)
		return nil
	})

	msg := fmt.Sprintf(`msg="final load stats" data_format=%q total=%d success=%d failed=%d total_rows_inserted=%s took=%s`,
		"JSONL", len(j.filesList), len(j.filesList)-int(failed), failed, shared.FormatNumber(totalRowsInserted), time.Since(start))
	return msg, err
}

// this was 7-14sec faster
func convertJsonlToCSV(w io.Writer, file string, cols []string) (err error) {
	r, err := reader.NewFileGzipReader(file)
	if err != nil {
		return err
	}
	defer r.Close()

	cw := csv.NewWriter(w)
	defer cw.Flush()

	// Write column headers first.
	if err = cw.Write(cols); err != nil {
		return err
	}

	ar := NewAsyncReader(r, cw, cols)
	go ar.parseRows()

	// Collect all errors and only return the first one.
	var firstErr error
	defer func() {
		close(ar.ErrCh)
		if firstErr != nil {
			err = firstErr
		}
	}()

	go func() {
		for e := range ar.ErrCh {
			if firstErr == nil {
				firstErr = e
			}
		}
	}()

	rows := make([][]string, batchSize)
	idx := 0
	for row := range ar.OutCh {
		rows[idx] = row
		idx++
		if idx == batchSize {
			idx = 0
			if err = cw.WriteAll(rows); err != nil {
				return err
			}
		}
	}
	if idx > 0 {
		return cw.WriteAll(rows[:idx])
	}
	return nil
}

// 4-10sec faster than convertJsonlToCSV2
func convertJsonlToCSV2(w io.Writer, file string, cols []string) (err error) {
	r, err := reader.NewFileGzipReader(file)
	if err != nil {
		return err
	}
	defer r.Close()
	return codes.ConvertJsonlToCsv(cols, r, w)
}

func toString(val any) string {
	switch t := val.(type) {
	case int:
		return strconv.Itoa(t)
	case float64:
		return fmt.Sprintf("%f", t)
	case string:
		return t
	// Handle JSON arrays and objects by converting them to strings.
	case []any, map[string]any:
		bt, _ := json.Marshal(t)
		return string(bt)
	case nil:
		return ""
	default:
		fmt.Println("entered into default for json files")
		return fmt.Sprintf("%s", val)
	}
}

func (j *JsonLoader) findTypesAndGetCols(file string) ([]string, []string, error) {
	r, err := reader.NewFileGzipReader(file)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	// Even though the type setting is text, we should read some rows to find all columns that exist.
	// In JSONL, a row might have fewer keys, while others might have more keys. So we need all of those keys.
	return shared.FindColumnTypes(r, j.lookUpSize, j.typeSetting)
}

func printError(f, name string, err error) {
	fmt.Printf(`status=FAILED data_format="JSONL" msg="unable to load" file=%q name=%q error=%q`+"\n", f, name, err.Error())
}
