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
*/
