package shared

import (
	"io"
	"strconv"
	"sync"

	clp "github.com/anvesh9652/concurrent-line-processor"
	"github.com/anvesh9652/pgload/internal/pgdb/dbv2"
	"github.com/buger/jsonparser"
)

const MaxRowsReadLimit = 25_000

// Takes a reader as a parameter where the data inside it is JSONL.
func FindColumnTypes(r io.Reader, rowsReadLimit int, typeSetting string) ([]string, []string, error) {
	rowsReadLimit = min(rowsReadLimit, MaxRowsReadLimit)

	// Column and respective types we have encountered.
	columnTypes := make(map[string]map[string]int)
	mut := sync.Mutex{}

	lineProcessor := func(b []byte) ([]byte, error) {
		mut.Lock()
		defer mut.Unlock()
		err := jsonparser.ObjectEach(b, func(key, value []byte, dataType jsonparser.ValueType, offset int) error {
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
			return nil, err
		}
		return b, nil
	}

	cr := clp.NewConcurrentLineProcessor(r,
		clp.WithWorkers(1024*1024*4), clp.WithWorkers(8),
		clp.WithRowsReadLimit(rowsReadLimit), clp.WithCustomLineProcessor(lineProcessor),
	)
	if _, err := io.Copy(io.Discard, cr); err != nil {
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
