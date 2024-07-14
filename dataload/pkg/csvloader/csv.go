package csvloader

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/anvesh9652/logstream/cmd/shared"
	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb"
)

var BatchSize = 150

// var LookupSize = 100

type CSVLoader struct {
	filesList  []string
	db         *pgdb.DB
	lookUpSize int
}

func NewCSVLoader(files []string, db *pgdb.DB, look int) *CSVLoader {
	return &CSVLoader{
		filesList:  files,
		db:         db,
		lookUpSize: look,
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
	err := shared.RunInParellel(10, c.filesList, func(file string) error {
		columnTypes, err := findColumnTypes(file, c.lookUpSize)
		if err != nil {
			return err
		}
		var columnAndTypes []string
		for col, tp := range columnTypes {
			columnAndTypes = append(columnAndTypes, col+" "+tp)
		}

		err = c.db.EnsureTable(getTableName(file), fmt.Sprintf("(%s)", strings.Join(columnAndTypes, ", ")))
		if err != nil {
			return err
		}
		return c.InsertRecordsInBatches(file)
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
			mapRecord[val] = record[i]
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

func getTableName(file string) string {
	pathSplit := strings.Split(file, "/")
	N := len(pathSplit)
	// we are sure that we will always have proper csv file name
	lastName := strings.Split(pathSplit[N-1], ".")[0]
	if len(pathSplit) == 1 {
		return lastName
	}
	return pathSplit[N-2] + "_" + lastName
}

func findColumnTypes(path string, lookupSize int) (map[string]string, error) {
	csvr, headers, err := NewCSVReaderAndColumns(path)
	if err != nil {
		return nil, err
	}

	var lookUpRows [][]string
	for range lookupSize {
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
			typesCnt[findType(val)] += 1
		}
		types[col] = maxRecordedType(typesCnt)
	}
	return types, nil
}

func printJson(val any) {
	bt, _ := json.MarshalIndent(val, "", "  ")
	fmt.Println(string(bt))
}

func maxRecordedType(types map[string]int) string {
	val, res := -1, "varchar"
	for k, v := range types {
		if v > val {
			val, res = v, k
		}
	}
	return res
}

func findType(val string) string {
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return "int"
	}
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return "float"
	}
	return "varchar"
}

// path := "/Users/agali/Desktop/Work/Product/bills-data/mca-to-ea/2023-12-actual-usage-details-part-0.csv"
