package pkg

import (
	"fmt"
	"path/filepath"
	"strings"

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
	flagsMapS := make(map[string]string)
	flagsMapB := make(map[string]bool)

	flags := c.cmd.Flags()
	flags.VisitAll(func(f *pflag.Flag) {
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

	// connUrl, err := flags.GetString("url")
	// if err != nil {
	// 	return err
	// }
	// dbName, err := flags.GetString("database")
	// if err != nil {
	// 	return err
	// }
	// schema, err := flags.GetString("schema")
	// if err != nil {
	// 	return err
	// }
	// user, err := flags.GetString("user")
	// if err != nil {
	// 	return err
	// }
	// pass, err := flags.GetString("pass")
	// if err != nil {
	// 	return err
	// }
	// reset, err := flags.GetBool("reset")
	// if err != nil {
	// 	return err
	// }

	dbUrl := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		flagsMapS[User], flagsMapS[Password], flagsMapS[URL], flagsMapS[Database])
	db, err := pgdb.NewPostgresDB(dbUrl, flagsMapS[Schema], !flagsMapB[Reset])
	if err != nil {
		return err
	}
	c.db = db
	r, _ := flags.GetBool("reset")
	fmt.Println(r)
	// err = c.db.EnsureTable("test", "(name varchar)")
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
	fmt.Println(filesList)
	return nil
	// return csvloader.NewCSVLoader(filesList, c.db).Run()
}
