package pkg

import (
	"log"

	"github.com/spf13/cobra"
)

var version = "1.0.0"

var rootCommand = cobra.Command{
	Use:     "load",
	Short:   "loads data into postgresql",
	Long:    "Loads the provides csv files data in postgres sql tables",
	Example: "",
	Version: version,
	Run: func(cmd *cobra.Command, args []string) {
		icmd := CommandInfo{cmd: cmd, args: args}
		err := icmd.validateParamsAndSetupDB()
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
	pflags.StringP("user", "u", "postgres", "user name")
	pflags.StringP("pass", "p", "", "password for given user name")
	pflags.StringP("database", "d", "postgres", "database name")
	pflags.StringP("schema", "s", "public", "schema name")
	pflags.StringP("url", "U", "localhost:5432", "connection string connect")
}
