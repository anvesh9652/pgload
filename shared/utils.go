package shared

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

const (
	Dynamic = "dynamic"
	AllText = "alltext"
)

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

func Check(err error, msg string, v ...any) {
	if err != nil {
		fmt.Fprintf(os.Stdout, msg, v...)
		fmt.Println()
		fmt.Fprintf(os.Stderr, err.Error(), v...)
		fmt.Println()
		os.Exit(1)
	}
}

func WriteToAsJson(data any, w io.Writer) {
	bytes, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err, "write to json is failed")
	}
	_, _ = w.Write(append(bytes, '\n'))
}
