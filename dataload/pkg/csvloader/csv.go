package csvloader

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb"
	"github.com/anvesh9652/side-projects/dataload/pkg/streams"
	"github.com/anvesh9652/side-projects/shared"
	"github.com/anvesh9652/side-projects/shared/csvutils"
)

var BatchSize = 400

type CSVLoader struct {
	MaxConcurrentRuns int

	filesList   []string
	db          *pgdb.DB
	lookUpSize  int
	typeSetting string
}

func NewCSVLoader(files []string, db *pgdb.DB, look int, t string, maxRuns int) *CSVLoader {
	return &CSVLoader{
		filesList:         files,
		db:                db,
		lookUpSize:        look,
		typeSetting:       t,
		MaxConcurrentRuns: maxRuns,
	}
}

func (c *CSVLoader) Run() error {
	err := shared.RunInParallel(c.MaxConcurrentRuns, c.filesList, func(file string) error {
		columnTypes, err := csvutils.FindColumnTypes(file, c.lookUpSize, &c.typeSetting)
		if err != nil {
			return err
		}
		columnAndTypes := csvutils.BuildColumnTypeStr(columnTypes)

		name := shared.GetTableName(file)
		err = c.db.EnsureTable(name, fmt.Sprintf("(%s)", strings.Join(columnAndTypes, ", ")))
		if err != nil {
			log.Printf("File: %s, name: %s, Error: %s\n,", file, name, err.Error())
			return err
		}
		err = c.InsertRecordsInBatches(file)
		if err != nil {
			log.Printf("File: %s, name: %s, Error: %s\n,", file, name, err.Error())
			return err
		}
		log.Printf("successfully loaded: %s\n", file)
		return nil
	})
	return err
}

func (c *CSVLoader) InsertRecordsInBatches(path string) error {
	r, headers, err := csvutils.NewCSVReaderAndColumns(path)
	if err != nil {
		return err
	}
	tableName := shared.GetTableName(path)
	recordsMap := []map[string]any{}
	for {
		mapRecord := map[string]any{}
		record, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		for i, val := range headers {
			mapRecord[val] = sql.NullString{String: record[i], Valid: record[i] != ""}
		}
		recordsMap = append(recordsMap, mapRecord)
		if len(recordsMap) == BatchSize {
			err = c.db.InsertRecords(tableName, recordsMap, headers)
			if err != nil {
				return err
			}
			recordsMap = []map[string]any{}
		}
	}
	if len(recordsMap) > 0 {
		return c.db.InsertRecords(tableName, recordsMap, headers)
	}
	return nil
}

func (c *CSVLoader) InsertRecordsInBatches2(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	headers, r, err := csvutils.GetCSVHeaders(f)
	if err != nil {
		return err
	}

	asyncReader := streams.StarChunksStreaming(r)
	tableName := shared.GetTableName(path)
	recordsMap := []map[string]any{}
	defer func() {
		close(asyncReader.Err)
	}()
	for record := range asyncReader.Out {
		if len(asyncReader.Err) > 0 {
			return <-asyncReader.Err
		}

		mapRecord := map[string]any{}
		for i, val := range headers {
			mapRecord[val] = sql.NullString{String: record[i], Valid: record[i] != ""}
		}
		// shared.WriteToAsJson(mapRecord, w)
		recordsMap = append(recordsMap, mapRecord)
		if len(recordsMap) == BatchSize {
			err = c.db.InsertRecords(tableName, recordsMap, headers)
			if err != nil {
				return err
			}
			recordsMap = []map[string]any{}
		}
	}
	if len(recordsMap) > 0 {
		return c.db.InsertRecords(tableName, recordsMap, headers)
	}
	return nil
}
