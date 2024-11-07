package main

import (
	"log"

	"github.com/anvesh9652/side-projects/dataload/pkg"
)

func main() {
	log.SetFlags(0)
	pkg.Execute()
}

/*
Bugs:
- running load immediately after deleting schema giving schema relation already exists error

- for some reason sometimes existing table schema was not being updated (esp jsonl) when we try to overwrite dynamic types table with
all text schema it's not working
*/
