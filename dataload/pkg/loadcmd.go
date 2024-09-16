package pkg

import (
	"log"

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
)

var rootCommand = cobra.Command{
	Use:     "load",
	Short:   "loads data into postgresql",
	Long:    "Loads the provides csv files data into postgres sql tables",
	Example: example,
	Version: version,
	Run: func(cmd *cobra.Command, args []string) {
		icmd := CommandInfo{cmd: cmd, args: args}
		err := icmd.setUpDBClient()
		if err != nil {
			log.Fatal(err)
		}
		err = icmd.RunCSVLoader()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func Execute() {
	err := rootCommand.Execute()
	if err != nil {
		log.Fatal(err)
	}
}

func init() {
	pflags := rootCommand.Flags()
	pflags.StringP(User, "U", "postgres", "user name")
	pflags.StringP(Password, "P", "", "password for given user name")
	pflags.StringP(Database, "d", "postgres", "database name")
	pflags.StringP(Schema, "s", "public", "schema name")
	pflags.StringP(URL, "u", "localhost:5432", "connection string to connect to the server")
	pflags.StringP(Port, "p", "", "postgres server localhost port number")

	// Boolean flags
	pflags.BoolP(Reset, "r", false, "reset tables if exists by default set to true")

	pflags.IntP(LookUp, "l", 400, "look first n number of rows to find column types")
}
