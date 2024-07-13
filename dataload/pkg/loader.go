package pkg

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

type CommandInfo struct {
	// cobra command
	cmd  *cobra.Command
	args []string

	useCreds bool

	db *DB
}

func (c *CommandInfo) validateParamsAndSetupDB() error {
	flagSet := c.cmd.Flags()
	userName, err := flagSet.GetString("user")
	if err != nil {
		return err
	}
	pass, err := flagSet.GetString("pass")
	if err != nil {
		return err
	}
	n, m := len(userName), len(pass)
	if n != 0 && m != 0 {
		fmt.Println("hi there")
		c.useCreds = true
	} else if n > 0 || m > 0 {
		return errors.New("provide both user name and password")
	}
	return c.setUpDBClient()
}

func (c *CommandInfo) setUpDBClient() error {
	flags := c.cmd.Flags()
	connUrl, err := flags.GetString("url")
	if err != nil {
		return err
	}
	dbName, err := flags.GetString("database")
	if err != nil {
		return err
	}
	schema, err := flags.GetString("schema")
	if err != nil {
		return err
	}
	user, _ := flags.GetString("user")
	pass, _ := flags.GetString("pass")
	// dbUrl := fmt.Sprintf("postgres://%s:@%s/%s?sslmode=disable", "postgres", connUrl, dbName)

	dbUrl := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, connUrl, dbName)
	fmt.Println(dbUrl)
	// if c.useCreds {
	// 	dbUrl = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, connUrl, dbName)
	// }
	db, err := NewPostgresDB(dbUrl, schema)
	if err != nil {
		return err
	}
	fmt.Println("sucess")
	c.db = db
	return nil
}

func NewLoader() {
}
