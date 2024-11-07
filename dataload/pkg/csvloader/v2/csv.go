package v2

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
	"github.com/anvesh9652/side-projects/shared"
	"github.com/anvesh9652/side-projects/shared/csvutils"
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
		f, err := os.Open(file)
		if err != nil {
			printError(file, name, err)
			return err
		}
		defer f.Close()
		rowsInserted, err := LoadCSV(ctx, f, name, c.db)
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

func LoadCSV(ctx context.Context, f io.Reader, table string, db *dbv2.DB) (int64, error) {
	headers, r, err := csvutils.GetCSVHeaders(f)
	if err != nil {
		return 0, err
	}

	// TODO: had to use this LATIN1 encoding, getting this error since we started using jsonparser:
	// invalid byte sequence for encoding \"UTF8\"
	// update: still we are getting same kind of error ERROR: invalid byte sequence for encoding \"LATIN1\":
	copyCmd := fmt.Sprintf(`COPY %s.%s(%s) FROM STDIN with ENCODING 'LATIN1' DELIMITER %s %s`,
		db.Schema(), table, strings.Join(headers, ", "), Delimiter, DataFormat,
	)
	return db.LoadIn(ctx, r, copyCmd)
}

func printError(f, name string, err error) {
	fmt.Printf(`status=FAILED data_format="CSV" msg="unable to load" file=%q name=%q error=%q`+"\n", f, name, err.Error())
}
