package shared

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"unicode"

	"github.com/dustin/go-humanize"
)

const (
	Dynamic = "dynamic"
	AllText = "alltext"
)

// data formats
var (
	CSV   = "csv"
	JSONL = "jsonl"
	Both  = "both"
)

func GetTableName(file string) string {
	// Table names are being created with lowercase letters
	// even if we pass uppercase letters
	file = strings.ToLower(file)
	pathSplit := strings.Split(file, "/")
	N := len(pathSplit)
	// we are sure that we will always have a proper file name that can be either .csv or .json or .gz
	// so no need to have any checks around idx
	name := getFileName(pathSplit[N-1])
	if len(pathSplit) > 1 {
		name = pathSplit[N-2] + "_" + name
	}
	if unicode.IsDigit(rune(name[0])) {
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

func getFileName(name string) string {
	ns := strings.Split(name, ".")
	if !IsGZIPFile(name) {
		return ns[0]
	}
	// file.csv.gz => file_gz
	return fmt.Sprintf("%s_%s", ns[0], ns[len(ns)-1])
}

func Check(err error, msg string, v ...any) {
	if err != nil {
		fmt.Fprintf(os.Stdout, msg, v...)
		fmt.Println()
		fmt.Fprintf(os.Stderr, err.Error(), v...)
		fmt.Println()
		os.Exit(1)
	}
}

func PrettyPrintJson(data any, w io.Writer) {
	if w == nil {
		w = os.Stdout
	}
	bytes, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		log.Fatal(err, "write to json is failed")
	}
	w.Write(bytes)
}

func GetFileSize(path string) (res string) {
	res = "unknown"
	f, err := os.Open(path)
	if err != nil {
		return
	}
	fi, err := f.Stat()
	if err != nil {
		return
	}
	return strings.ReplaceAll(humanize.Bytes(uint64(fi.Size())), " ", "")
}

func FormatNumber(n int64) string {
	decimalPoint := 2
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.*fM", decimalPoint, float64(n)/1_000_000)
	case n >= 1_000:
		if n%1000 == 0 {
			return fmt.Sprintf("%dk", n/1000)
		}
		val := fmt.Sprintf("%.*f", decimalPoint, float64(n)/1_000)
		return strings.TrimSuffix(val, "0") + "k"
	default:
		return fmt.Sprintf("%d", n)
	}
}

func RunInParallel(numWorkers int, items []string, fn func(item string) error) error {
	n := len(items)
	workers := min(numWorkers, n)

	var (
		workerErrOnce sync.Once
		workerErr     error

		wg        = new(sync.WaitGroup)
		itemsChan = make(chan string, n)
	)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx := range itemsChan {
				if err := fn(ctx); err != nil {
					workerErrOnce.Do(func() {
						workerErr = err
					})
				}
			}
		}()
	}

	for _, item := range items {
		itemsChan <- item
	}
	close(itemsChan)
	wg.Wait()
	return workerErr
}

func IsGZIPFile(name string) bool {
	return strings.HasSuffix(name, ".gz")
}

func IsCSVFile(name string) bool {
	if strings.HasSuffix(name, ".csv") {
		return true
	}
	ns := strings.Split(name, ".")
	return IsGZIPFile(name) && len(ns) >= 3 && ns[len(ns)-2] == "csv"
}

func IsJSONFile(name string) bool {
	if strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".jsonl") {
		return true
	}
	ns := strings.Split(name, ".")
	return IsGZIPFile(name) && len(ns) > 2 && (ns[len(ns)-2] == "json" || ns[len(ns)-2] == "jsonl")
}
