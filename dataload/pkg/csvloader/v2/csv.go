package v2

import (
    "context"
    "fmt"
    "os"
    "strings"
    "sync/atomic"

    "github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
    "github.com/anvesh9652/side-projects/shared"
    "github.com/anvesh9652/side-projects/shared/csvutils"
    stlogs "github.com/anvesh9652/streamlogs/shared"
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

func (c *CSVLoader) Run(ctx context.Context) error {
    var failed int32
    err := stlogs.RunInParellel(c.MaxConcurrentRuns, c.filesList, func(file string) error {
        var err error
        defer func() {
            if err != nil {
                atomic.AddInt32(&failed, int32(1))
            }
        }()

        name := shared.GetTableName(file)
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
        rowsInserted, err := c.load(ctx, file, name)
        if err != nil {
            printError(file, name, err)
            return err
        }
        fmt.Printf("status=SUCCESS rows_inserted=%d file_size=%s file=%s\n", rowsInserted, shared.GetFileSize(file), file)
        return nil
    })
    fmt.Printf("msg=\"final load stats\" total=%d success=%d failed=%d\n", len(c.filesList), len(c.filesList)-int(failed), failed)
    return err
}

func (c *CSVLoader) load(ctx context.Context, f, table string) (int64, error) {
    file, err := os.Open(f)
    if err != nil {
        return 0, err
    }
    headers, r, err := csvutils.GetCSVHeaders(file)
    if err != nil {
        return 0, err
    }
    copyCmd := fmt.Sprintf(`COPY %s.%s(%s) FROM STDIN with DELIMITER %s %s`,
        c.db.Schema(), table, strings.Join(headers, ", "), Delimiter, DataFormat,
    )
    return c.db.LoadIn(ctx, r, copyCmd)
}

func printError(f, name string, err error) {
    fmt.Printf(`status=FAILED msg="unable to load" file=%q name=%q error=%q`, f, name, err.Error())
}
