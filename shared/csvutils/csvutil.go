package csvutils

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
	"github.com/anvesh9652/side-projects/shared"
	"github.com/anvesh9652/side-projects/shared/reader"
)

func NewCSVReaderAndColumns(path string) (*csv.Reader, []string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	csvr := csv.NewReader(file)
	headers, err := csvr.Read()
	return csvr, headers, err
}

func BuildColumnTypeStr(types map[string]string) (res []string) {
	for col, tp := range types {
		res = append(res, col+" "+tp)
	}
	return
}

func FindColumnTypes(path string, lookUpSize int, typeSetting *string) (map[string]string, error) {
	r, err := reader.NewFileGzipReader(path)
	if err != nil {
		return nil, err
	}
	r.Close()
	csvr := csv.NewReader(r)
	headers, err := csvr.Read()
	if err != nil {
		return nil, err
	}

	var lookUpRows [][]string
	for range lookUpSize {
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
				typesCnt[findType(val, typeSetting)] += 1
			}
		}
		types[col] = maxRecordedType(typesCnt)
	}
	return types, nil
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

func findType(val string, typeSetting *string) string {
	if typeSetting != nil && *typeSetting == shared.AllText {
		return dbv2.Text
	}

	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return dbv2.Integer
	}
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return dbv2.Float
	}
	return dbv2.Text
}

// 
func GetCSVHeaders(r io.Reader) ([]string, io.Reader, error) {
	// didn't find the best way to get only first row
	// no need to worry here if `br` reads more than first row
	br := bufio.NewReader(r)
	buff := bytes.NewBuffer(nil)
	for {
		line, prefix, err := br.ReadLine()
		if err != nil {
			return nil, nil, err
		}
		buff.Write(line)
		if !prefix {
			break
		}
	}
	csvR := csv.NewReader(buff)
	headers, err := csvR.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read first line: %v", err)
	}
	return headers, br, nil
}
