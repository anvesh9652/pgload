package main

import (
	"log"

	"github.com/anvesh9652/pgload/internal"
)

func main() {
	log.SetFlags(0)
	internal.Execute()
}

/*
Bugs:

Features:
Intelligent sampling:
	- read beginning, middle and end, and also btw beg-mid and mid-end(uncommon parts)
*/
