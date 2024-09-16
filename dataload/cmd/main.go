package main

import (
	"fmt"
	"log"
	"time"

	"github.com/anvesh9652/side-projects/dataload/pkg"
)

func main() {
	start := time.Now()
	log.SetFlags(0)
	pkg.Execute()
	fmt.Println("took:", time.Since(start))
}
