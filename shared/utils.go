package shared

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"unicode"
)

const (
	Dynamic = "dynamic"
	AllText = "alltext"
)

func GetTableName(file string) string {
	// Table names are being created with lowercase letters
	// even if we pass uppercase letters
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
