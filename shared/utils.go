package shared

import (
	"fmt"
	"os"
)

func Check(err error, msg string, v ...any) {
	if err != nil {
		fmt.Fprintf(os.Stdout, msg, v...)
		fmt.Println()
		fmt.Fprintf(os.Stderr, err.Error(), v...)
		fmt.Println()
		os.Exit(1)
	}
}
