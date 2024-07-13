package pkg

import (
	"log"

	"github.com/spf13/cobra"
)

var version = "1.0.0"

const (
	User     = "user"
	Password = "pass"
	Database = "database"
	Schema   = "schema"
	URL      = "url"
	Reset    = "reset"
)

var rootCommand = cobra.Command{
	Use:     "load",
	Short:   "loads data into postgresql",
	Long:    "Loads the provides csv files data in postgres sql tables",
	Example: "",
	Version: version,
	Run: func(cmd *cobra.Command, args []string) {
		icmd := CommandInfo{cmd: cmd, args: args}
		err := icmd.setUpDBClient()
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
	// intialize flags here later
	pflags := rootCommand.PersistentFlags()
	pflags.StringP(User, "u", "postgres", "user name")
	pflags.StringP(Password, "p", "", "password for given user name")
	pflags.StringP(Database, "d", "postgres", "database name")
	pflags.StringP(Schema, "s", "public", "schema name")
	pflags.StringP(URL, "U", "localhost:5432", "connection string connect")
	pflags.BoolP(Reset, "r", false, "reset tables if exists by default it's true")
}
