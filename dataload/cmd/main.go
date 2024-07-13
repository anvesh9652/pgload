package main

import (
	"log"

	"github.com/anvesh9652/side-projects/dataload/pkg"
)

/*

-u = user
-p = pass
-g = glob pattern

-d = db name
-s = schema name
-U = connection url includes port

files list if -g not present

*/

func main() {
	log.SetFlags(0)
	pkg.Execute()
}
