package v2

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"time"

	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
	"github.com/anvesh9652/side-projects/shared"
	"github.com/anvesh9652/side-projects/shared/csvutils"
	"github.com/anvesh9652/side-projects/shared/reader"
)

const (
	DataFormat = "CSV"
	Delimiter  = `','`
)

type CSVLoader struct {
	MaxConcurrentRuns int

	filesList   []string
	db          *dbv2.DB
	lookUpSize  int
	typeSetting string
}

func NewCSVLoader(files []string, db *dbv2.DB, look int, t string, maxRuns int) *CSVLoader {
	return &CSVLoader{
		filesList:         files,
		db:                db,
		lookUpSize:        look,
		typeSetting:       t,
		MaxConcurrentRuns: maxRuns,
	}
}

func (c *CSVLoader) Run(ctx context.Context) (string, error) {
	var totalRowsInserted, failed int64

	start := time.Now()
	err := shared.RunInParallel(c.MaxConcurrentRuns, c.filesList, func(file string) error {
		var err error
		name := shared.GetTableName(file)

		defer func() {
			if err != nil {
				atomic.AddInt64(&failed, int64(1))
				_ = c.db.DeleteTable(name)
			}
		}()

		columnTypes, err := csvutils.FindColumnTypes(file, c.lookUpSize, &c.typeSetting)
		if err != nil {
			printError(file, name, err)
			return err
		}
		columnAndTypes := csvutils.BuildColumnTypeStr(columnTypes)

		err = c.db.EnsureTable(name, fmt.Sprintf("(%s)", strings.Join(columnAndTypes, ", ")))
		if err != nil {
			printError(file, name, err)
			return err
		}

		r, err := reader.NewFileGzipReader(file)
		if err != nil{
			printError(file, name, err)
			return err
		}
		defer r.Close()

		rowsInserted, err := LoadCSV(ctx, r, name, c.db)
		if err != nil {
			printError(file, name, err)
			return err
		}
		atomic.AddInt64(&totalRowsInserted, rowsInserted)
		fmt.Printf("status=SUCCESS rows_inserted=%s file_size=%s file=%s\n",
			shared.FormatNumber(rowsInserted), shared.GetFileSize(file), file)
		return nil
	})
	msg := fmt.Sprintf(`msg="final load stats" data_format=%q total=%d success=%d failed=%d total_rows_inserted=%s took=%s`,
		"CSV", len(c.filesList), len(c.filesList)-int(failed), failed, shared.FormatNumber(totalRowsInserted), time.Since(start))
	return msg, err
}

func LoadCSV(ctx context.Context, r io.Reader, table string, db *dbv2.DB) (int64, error) {
	headers, r, err := csvutils.GetCSVHeaders(r)
	if err != nil {
		return 0, err
	}
	copyCmd := fmt.Sprintf(`COPY %s.%s(%s) FROM STDIN with DELIMITER %s %s`,
		db.Schema(), table, strings.Join(headers, ", "), Delimiter, DataFormat,
	)
	return db.LoadIn(ctx, r, copyCmd)
}

func printError(f, name string, err error) {
	fmt.Printf(`status=FAILED data_format="CSV" msg="unable to load" file=%q name=%q error=%q`+"\n", f, name, err.Error())
}
