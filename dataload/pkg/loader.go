package pkg

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	csvloader "github.com/anvesh9652/side-projects/dataload/pkg/csvloader/v2"
	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
	"github.com/anvesh9652/side-projects/shared"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

const (
	concurrentRuns = 8
)

type CommandInfo struct {
	// cobra command
	cmd  *cobra.Command
	args []string

	db *dbv2.DB
}

func (c *CommandInfo) setUpDBClient(ctx context.Context) error {
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
				log.Printf("Error while retrieving %s flag value\n", f.Name)
			}
			flagsMapB[f.Name] = val
		}
	})

	url := flagsMapS[URL]
	if flagsMapS[Port] != "" {
		url = "localhost:" + flagsMapS[Port]
	}

	dbUrl := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable", flagsMapS[User],
		flagsMapS[Password], url, flagsMapS[Database],
	)

	c.db, err = dbv2.NewPostgresDB(ctx, dbUrl, flagsMapS[Schema], !flagsMapB[Reset])
	return err
}

func (c *CommandInfo) RunCSVLoader(ctx context.Context) error {
	var filesList []string
	for _, arg := range c.args {
		if strings.Contains(arg, "*") {
			result, err := filepath.Glob(arg)
			if err != nil {
				return errors.Wrapf(err, "glob pattern matching failed: %s", arg)
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
	lookUp, err := c.cmd.Flags().GetInt(LookUp)
	if err != nil {
		return err
	}
	typeSetting, err := c.cmd.Flags().GetString(Type)
	if err != nil {
		return err
	}
	if typeSetting != shared.Dynamic && typeSetting != shared.AllText {
		return fmt.Errorf("unknown value for type %q", typeSetting)
	}
	if len(filesList) == 0 {
		return errors.New("atleast provide one file")
	}
	return csvloader.NewCSVLoader(filesList, c.db, lookUp, typeSetting, concurrentRuns).Run(ctx)
}
