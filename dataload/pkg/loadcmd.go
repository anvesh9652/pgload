package pkg

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/anvesh9652/side-projects/shared"
	"github.com/spf13/cobra"
)

var (
	version = "1.0.0"
	example = `1. load file1.csv file2.csv file3.csv
2. load -p 54321 data.csv 
3. load -U test -P 123 -d temp -s testing -u "localhost:123" file_2*.csv test1.csv dummy/*/*.csv`
)

const (
	User     = "user"
	Password = "pass"
	Database = "database"
	Schema   = "schema"
	URL      = "url"
	Port     = "port"
	Reset    = "reset"
	LookUp   = "lookup"
	Type     = "type"
)

var rootCommand = cobra.Command{
	Use:     "load",
	Short:   "Efficiently loads data into PostgreSQL",
	Long:    "Loads the provided CSV files data into PostgreSQL tables, leveraging optimized processes for faster performance.",
	Example: example,
	Version: version,
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Now()
		defer func() {
			fmt.Println("took=", time.Since(start))
		}()
		icmd := CommandInfo{cmd: cmd, args: args}
		ctx := context.Background()
		err := icmd.setUpDBClient(ctx)
		failOnError(err)
		err = icmd.RunCSVLoader(ctx)
		failOnError(err)
	},
}

func Execute() {
	err := rootCommand.Execute()
	if err != nil {
		log.Fatal(err)
	}
}

func failOnError(err error) {
	if err == nil {
		return
	}
	fmt.Fprint(os.Stderr, err.Error())
	os.Exit(1)
}

func init() {
	pflags := rootCommand.Flags()
	pflags.StringP(User, "U", "postgres", "user name")
	pflags.StringP(Password, "P", "", "password for given user name")
	pflags.StringP(Database, "d", "postgres", "database name")
	pflags.StringP(Schema, "s", "public", "schema name")
	pflags.StringP(URL, "u", "localhost:5432", "connection string to connect to the server")
	pflags.StringP(Port, "p", "", "postgres server localhost port number")
	pflags.StringP(Type, "t", shared.Dynamic, "setting(dynamic, alltext) used to assign type for table columns")

	// Boolean flags
	pflags.BoolP(Reset, "r", false, "reset tables if exists by default set to true")

	pflags.IntP(LookUp, "l", 400, "look first n number of rows to find column types")
}
