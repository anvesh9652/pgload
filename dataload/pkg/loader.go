package pkg

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/anvesh9652/side-projects/dataload/pkg/csvloader"
	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type CommandInfo struct {
	// cobra command
	cmd  *cobra.Command
	args []string

	db *pgdb.DB
}

func (c *CommandInfo) setUpDBClient() error {
	var (
		err error

		flagsMapS = make(map[string]string)
		flagsMapB = make(map[string]bool)
	)

	flags := c.cmd.Flags()
	flags.VisitAll(func(f *pflag.Flag) {
		// we only need string and bool flag values
		switch f.Value.Type() {
		case "string":
			flagsMapS[f.Name] = f.Value.String()
		case "bool":
			val, err := flags.GetBool(f.Name)
			if err != nil {
				fmt.Printf("Error while retrieving %s flag value\n", f.Name)
			}
			flagsMapB[f.Name] = val
		}
	})

	dbUrl := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable", flagsMapS[User],
		flagsMapS[Password], flagsMapS[URL], flagsMapS[Database],
	)

	c.db, err = pgdb.NewPostgresDB(dbUrl, flagsMapS[Schema], !flagsMapB[Reset])
	// c.db.EnsureTable("test", "(name varchar)")
	return err
}

func (c *CommandInfo) RunCSVLoader() error {
	var filesList []string
	for _, arg := range c.args {
		if strings.Contains(arg, "*") {
			result, err := filepath.Glob(arg)
			if err != nil {
				return errors.Wrapf(err, "failed for glob patter: %s", arg)
			}
			for _, file := range result {
				if strings.HasSuffix(file, ".csv") {
					filesList = append(filesList, file)
				}
			}
		} else {
			if strings.HasSuffix(arg, ".csv") {
				filesList = append(filesList, arg)
			}
		}
	}
	return csvloader.NewCSVLoader(filesList, c.db).Run()
}
