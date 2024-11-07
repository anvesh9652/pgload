package jsonloader

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	csv2 "github.com/anvesh9652/side-projects/dataload/pkg/csvloader/v2"
	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
	"github.com/anvesh9652/side-projects/shared"
	"github.com/sourcegraph/conc/pool"
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

func convertJsonlToCSV(w io.Writer, file string, cols []string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	cw := csv.NewWriter(w)
	defer cw.Flush()

	// write cols first
	if err = cw.Write(cols); err != nil {
		return err
	}
	dec := json.NewDecoder(f)
	return writeAsCSV(cw, dec, cols)
}

// this was 7-14sec faster
func convertJsonlToCSV2(w io.Writer, file string, cols []string) (err error) {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	cw := csvWriterWrapper{orgCsvW: csv.NewWriter(w)}
	defer cw.Flush()

	// write cols first
	if err = cw.Write(cols); err != nil {
		return err
	}

	ar := NewAsyncReader(f, cols)
	go ar.parseRows()

	// collect all the errors and only print the actual error
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

// make use of bulk writes
func writeAsCSV(cw *csv.Writer, dec *json.Decoder, cols []string) error {
	var err error
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
	switch t := val.(type) {
	case int:
		return strconv.Itoa(t)
	case float64:
		return fmt.Sprintf("%f", t)
	// todo: why this isn't working by using this?
	// case string:
	// 	return t
	case any:
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
			types = append(types, header+" "+dbv2.Text)
			continue
		}

		uniqueTypes := map[string]int{}
		for _, row := range rows {
			val := row[header]
			uniqueTypes[getType(val)]++
		}

		types = append(types, header+" "+maxRecordedType(uniqueTypes))
	}
	return types, colsList, nil
}

func getType(val any) string {
	switch val.(type) {
	case float64, int:
		return dbv2.Float
	case string:
		return dbv2.Text
	case any:
		return dbv2.Json
	default:
		return dbv2.Text
	}
}

func maxRecordedType(types map[string]int) string {
	if types[dbv2.Text] > 0 {
		return dbv2.Text
	}
	val, res := -1, dbv2.Text
	for k, v := range types {
		if v > val {
			val, res = v, k
		}
	}
	return res
}

func printError(f, name string, err error) {
	fmt.Printf(`status=FAILED data_format="JSONL" msg="unable to load" file=%q name=%q error=%q`+"\n", f, name, err.Error())
}

// this are debug purpose just see what exactly we are loading
type csvWriterWrapper struct {
	orgCsvW *csv.Writer
	debugW  *csv.Writer
}

func (t *csvWriterWrapper) Write(r []string) error {
	if t.debugW != nil {
		_ = t.debugW.Write(r)
	}
	return t.orgCsvW.Write(r)
}

func (t *csvWriterWrapper) WriteAll(r [][]string) error {
	if t.debugW != nil {
		_ = t.debugW.WriteAll(r)
	}
	return t.orgCsvW.WriteAll(r)
}

func (t *csvWriterWrapper) Flush() {
	if t.debugW != nil {
		t.debugW.Flush()
	}
	t.orgCsvW.Flush()
}
