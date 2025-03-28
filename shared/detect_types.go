package shared

import (
	"bufio"
	"io"
	"strconv"

	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
	"github.com/buger/jsonparser"
)

const MaxRowsReadLimit = 10_000

// Takes a reader as a parameter where the data inside it is JSONL.
func FindColumnTypes(r io.Reader, rowsReadLimit int, typeSetting string) ([]string, []string, error) {
	rowsReadLimit = min(rowsReadLimit, MaxRowsReadLimit)

	// Column and respective types we have encountered.
	columnTypes := make(map[string]map[string]int)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		data := scanner.Bytes()
		err := jsonparser.ObjectEach(data, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {
			keyString := string(key)

			if _, exists := columnTypes[keyString]; !exists {
				columnTypes[keyString] = make(map[string]int)
			}
			types := columnTypes[keyString]
			switch dataType {
			case jsonparser.Number:
				types[dbv2.Numeric]++
			case jsonparser.Null:
				// Just ignore the type detection for this value.
			case jsonparser.Array, jsonparser.Object:
				types[dbv2.Json]++
			default:
				types[dbv2.Text]++
			}

			columnTypes[keyString] = types
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		rowsReadLimit--
		if rowsReadLimit == 0 {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	var colsList []string
	var types []string

	for col, recordedTypes := range columnTypes {
		colsList = append(colsList, col)
		col = strconv.Quote(col)
		if typeSetting == AllText {
			types = append(types, col+" "+dbv2.Text)
			continue
		}

		types = append(types, col+" "+maxRecordedType(recordedTypes))
	}
	return types, colsList, nil
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
