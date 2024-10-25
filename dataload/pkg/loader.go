package pkg

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	builterr "errors"

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

type Flags map[string]any

type CommandInfo struct {
	// cobra command
	cmd  *cobra.Command
	args []string

	flagsMapS map[string]string
	flagsMapI map[string]int
	flagsMapB map[string]bool

	db *dbv2.DB
}

func NewCommandInfo(ctx context.Context, cmd *cobra.Command, args []string) (*CommandInfo, error) {
	c := &CommandInfo{
		cmd:       cmd,
		args:      args,
		flagsMapS: make(map[string]string),
		flagsMapB: make(map[string]bool),
		flagsMapI: make(map[string]int),
	}

	flags := c.cmd.Flags()
	var visitErrors []error
	flags.VisitAll(func(f *pflag.Flag) {
		// we only need string and bool flag values
		switch f.Value.Type() {
		case "string":
			c.flagsMapS[f.Name] = f.Value.String()
		case "int":
			val, err := flags.GetInt(LookUp)
			if err != nil {
				log.Printf("Error while retrieving %s flag value\n", f.Name)
				visitErrors = append(visitErrors, err)
			}
			c.flagsMapI[f.Name] = val
		case "bool":
			val, err := flags.GetBool(f.Name)
			if err != nil {
				log.Printf("Error while retrieving %s flag value\n", f.Name)
				visitErrors = append(visitErrors, err)
			}
			c.flagsMapB[f.Name] = val
		}
	})
	if len(visitErrors) > 0 {
		return nil, builterr.Join(visitErrors...)
	}

	url := c.flagsMapS[URL]
	if c.flagsMapS[Port] != "" {
		url = "localhost:" + c.flagsMapS[Port]
	}

	dbUrl := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable", c.flagsMapS[User],
		c.flagsMapS[Password], url, c.flagsMapS[Database],
	)

	var err error
	c.db, err = dbv2.NewPostgresDB(ctx, dbUrl, c.flagsMapS[Schema], !c.flagsMapB[Reset])
	return c, err
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
	lookUp, typeSetting := c.flagsMapI[LookUp], c.flagsMapS[Type]
	if typeSetting != shared.Dynamic && typeSetting != shared.AllText {
		return fmt.Errorf("unknown value for type %q", typeSetting)
	}
	if len(filesList) == 0 {
		return errors.New("atleast provide one file")
	}
	return csvloader.NewCSVLoader(filesList, c.db, lookUp, typeSetting, concurrentRuns).Run(ctx)
}
