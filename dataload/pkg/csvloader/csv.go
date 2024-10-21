package csvloader

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb"
	"github.com/anvesh9652/side-projects/dataload/pkg/streams"
	"github.com/anvesh9652/side-projects/shared"
	stlogs "github.com/anvesh9652/streamlogs/shared"
)

var BatchSize = 400

var (
	Integer = "INTEGER"
	Float   = "FLOAT"
	Text    = "TEXT"
)

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

func NewCSVReaderAndColumns(path string) (*csv.Reader, []string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	csvr := csv.NewReader(file)
	headers, err := csvr.Read()
	return csvr, headers, err
}

func (c *CSVLoader) Run() error {
	err := stlogs.RunInParellel(c.MaxConcurrentRuns, c.filesList, func(file string) error {
		columnTypes, err := c.findColumnTypes(file)
		if err != nil {
			return err
		}
		var columnAndTypes []string
		for col, tp := range columnTypes {
			columnAndTypes = append(columnAndTypes, col+" "+tp)
		}

		name := getTableName(file)
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
	r, headers, err := NewCSVReaderAndColumns(path)
	if err != nil {
		return err
	}
	tableName := getTableName(path)
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
	headers, r, err := shared.GetCSVHeaders(f)
	if err != nil {
		return err
	}

	asyncReader := streams.StarChunksStreaming(r)
	tableName := getTableName(path)
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

func getTableName(file string) string {
	// tables names are getting created with lower case letters
	// even we pass upper case letters
	file = strings.ToLower(file)
	pathSplit := strings.Split(file, "/")
	N := len(pathSplit)
	// we are sure that we will always have proper csv file name
	name := strings.Split(pathSplit[N-1], ".")[0]
	if len(pathSplit) > 1 {
		name = pathSplit[N-2] + "_" + name
	} else if unicode.IsDigit(rune(name[0])) {
		// we can't have a table name that start's with digit
		name = "t" + name
	}
	var final string
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			final += "_"
			continue
		}
		final += string(r)
	}
	return final
}

func (c *CSVLoader) findColumnTypes(path string) (map[string]string, error) {
	csvr, headers, err := NewCSVReaderAndColumns(path)
	if err != nil {
		return nil, err
	}

	var lookUpRows [][]string
	for range c.lookUpSize {
		record, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		lookUpRows = append(lookUpRows, record)
	}

	rowsCount := len(lookUpRows)
	types := map[string]string{}
	for i, col := range headers {
		typesCnt := map[string]int{}
		for ix := range rowsCount {
			val := lookUpRows[ix][i]
			if val != "" {
				typesCnt[findType(val, c.typeSetting)] += 1
			}
		}
		types[col] = maxRecordedType(typesCnt)
	}
	return types, nil
}

// For debuggin
func printJson(val any) {
	bt, _ := json.MarshalIndent(val, "", "  ")
	fmt.Println(string(bt))
}

func maxRecordedType(types map[string]int) string {
	if types[Text] > 0 {
		return Text
	}
	val, res := -1, "TEXT"
	for k, v := range types {
		if v > val {
			val, res = v, k
		}
	}
	return res
}

func findType(val, typeSetting string) string {
	if typeSetting == shared.AllText {
		return Text
	}
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return Integer
	}
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return Float
	}
	return Text
}
