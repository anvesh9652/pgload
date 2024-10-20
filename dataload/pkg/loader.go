package pkg

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anvesh9652/side-projects/dataload/pkg/csvloader"
	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb"
	"github.com/anvesh9652/side-projects/shared"
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

	c.db, err = pgdb.NewPostgresDB(dbUrl, flagsMapS[Schema], !flagsMapB[Reset])
	return err
}

func (c *CommandInfo) RunCSVLoader() error {
	start := time.Now()
	defer func() {
		fmt.Println("took:", time.Since(start))
	}()

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
	concurrentRuns := 10
	// check if any file is larger, and reduce the concurrent runs
	// and process the large in chucks to make it faster
	for _, file := range filesList {
		f, err := os.Open(file)
		shared.Check(err, "failed to open file: %s", file)
		info, err := f.Stat()
		shared.Check(err, "error getting file stats: %s", file)
		// 400 MB
		if info.Size() > 100*1024*1024 {
			concurrentRuns = 4
			break
		}
	}
	if len(filesList) == 0 {
		return errors.New("atleast provide one file")
	}
	fmt.Println(filesList)
	return csvloader.NewCSVLoader(filesList, c.db, lookUp, typeSetting, concurrentRuns).Run()
}
