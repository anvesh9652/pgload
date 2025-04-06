package internal

import (
	"context"
	"fmt"
	"log"
	"os"

	. "github.com/anvesh9652/pgload/pkg/shared"
	"github.com/spf13/cobra"
)

var (
	version = "1.0.0"
	example = `1. load file1.csv file2.csv file3.csv.gz
2. load -f jsonl file1.json file2.jsonl file3.json.gz
3. load -p 54321 data.csv
4. load -f both -p 54321 data.csv data.json all_files/*
5. load -U test -P 123 -d temp -s testing -u "localhost:123" file_2*.csv test1.csv dummy/*/*.csv`
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
	Format   = "format"
)

var rootCommand = cobra.Command{
	Use:     "load",
	Short:   "Efficiently loads data into PostgreSQL",
	Long:    "Loads the provided CSV and JSONL files data into PostgreSQL tables, leveraging optimized processes for faster performance.",
	Example: example,
	Version: version,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		icmd, err := NewCommandInfo(ctx, cmd, args)
		failOnError(err)
		err = icmd.RunLoader(ctx)
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
	fmt.Fprintln(os.Stderr, err.Error())
	os.Exit(1)
}

func init() {
	pflags := rootCommand.Flags()
	pflags.StringP(User, "U", "postgres", "user name")
	pflags.StringP(Password, "P", "", "password for given user name")
	pflags.StringP(Database, "d", "postgres", "database name")
	pflags.StringP(Schema, "s", "public", "schema name")
	pflags.StringP(URL, "u", "localhost:5432", "connection string to connect to the server")
	pflags.StringP(Port, "p", "", "Postgres server localhost port number")
	pflags.StringP(Type, "t", Dynamic, "setting (dynamic, alltext) used to assign type for table columns")
	pflags.StringP(Format, "f", CSV, fmt.Sprintf("the format of the data that is being loaded. Supports: %s, %s, %s", CSV, JSONL, Both))

	// Reset tables if they exist; by default, set to true.
	pflags.BoolP(Reset, "r", false, "reset tables if they exist; by default, set to true")

	pflags.IntP(LookUp, "l", 400, "looks up first n number of rows to find column types")
}
