package pkg

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"

	builterr "errors"

	csvloader "github.com/anvesh9652/side-projects/dataload/pkg/csvloader/v2"
	"github.com/anvesh9652/side-projects/dataload/pkg/jsonloader"
	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb/dbv2"
	"github.com/anvesh9652/side-projects/shared"
	"github.com/pkg/errors"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
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

func (c *CommandInfo) RunLoader(ctx context.Context) error {
	var csvFiles, jsonFiles []string
	for _, arg := range c.args {
		if strings.Contains(arg, "*") {
			result, err := filepath.Glob(arg)
			if err != nil {
				return errors.Wrapf(err, "glob pattern matching failed: %s", arg)
			}
			for _, file := range result {
				if strings.HasSuffix(file, ".csv") {
					csvFiles = append(csvFiles, file)
				}
				if strings.HasSuffix(file, ".json") {
					jsonFiles = append(jsonFiles, file)
				}
			}
		} else {
			if strings.HasSuffix(arg, ".csv") {
				csvFiles = append(csvFiles, arg)
			}
			if strings.HasSuffix(arg, ".json") {
				jsonFiles = append(jsonFiles, arg)
			}
		}
	}
	return c.RunFormatSpecificLoaders(ctx, csvFiles, jsonFiles)
}

func (c *CommandInfo) RunFormatSpecificLoaders(ctx context.Context, cf, jf []string) error {
	if len(cf)+len(jf) == 0 {
		return errors.New("at least provide one file")
	}

	lookUp, typeSetting := c.flagsMapI[LookUp], c.flagsMapS[Type]
	if typeSetting != shared.Dynamic && typeSetting != shared.AllText {
		return fmt.Errorf("unknown value for type %q", typeSetting)
	}

	mu := new(sync.Mutex)
	msgs := []string{}
	pool := pool.New().WithErrors()
	if len(cf) > 0 {
		pool.Go(func() error {
			msg, err := csvloader.NewCSVLoader(cf, c.db, lookUp, typeSetting, concurrentRuns).Run(ctx)
			mu.Lock()
			msgs = append(msgs, msg)
			mu.Unlock()
			return err
		})
	}
	if len(jf) > 0 {
		pool.Go(func() error {
			msg, err := jsonloader.New(jf, c.db, concurrentRuns, lookUp, typeSetting).Run(ctx)
			mu.Lock()
			msgs = append(msgs, msg)
			mu.Unlock()
			return err
		})
	}

	if err := pool.Wait(); err != nil {
		return err
	}
	fmt.Println(strings.Join(msgs, "\n"))
	return nil
}
