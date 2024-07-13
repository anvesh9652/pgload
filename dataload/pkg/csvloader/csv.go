package csvloader

import (
	"github.com/anvesh9652/side-projects/dataload/pkg/pgdb"
)

type CSVLoader struct {
	filesList []string
	db        *pgdb.DB
}

func NewCSVLoader(files []string, db *pgdb.DB) *CSVLoader {
	return &CSVLoader{
		filesList: files,
		db:        db,
	}
}

func (c *CSVLoader) Run() error {
	return nil
}
