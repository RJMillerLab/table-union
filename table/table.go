package table

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

var (
	ErrEmptyTable   = errors.New("Empty table")
	ErrNoTableFound = errors.New("No such table found")
)

type Header struct {
	IsNum bool   `json:"isNumeric"`
	Text  string `json:"text"`
}

// Table is a in-memory representation of a wikitable or an open table.
type Table struct {
	ID      string     `json:"id"`
	Headers []Header   `json:"headers"`
	Columns [][]string `json:"columns"`
}

// TableStore is provides an interface for the file system
// directory storing the wikitable or open table CSV files.
type TableStore struct {
	tableDir string
}

func NewTableStore(tableDir string) *TableStore {
	return &TableStore{
		tableDir: tableDir,
	}
}

// GetTable gets a table given its ID.
func (ts *TableStore) GetTable(id string) (*Table, error) {
	p := ts.getTableFilename(id)
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, ErrNoTableFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t, err := FromCSV(f)
	if err != nil {
		return nil, err
	}
	t.ID = id
	return t, nil
}

// Converts a CSV file to table.
func FromCSV(file io.Reader) (*Table, error) {
	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, errors.New("Empty table")
	}
	// Make headers
	headers := make([]Header, len(rows[0]))
	for i := range headers {
		isNum, err := strconv.ParseBool(rows[1][i])
		if err != nil {
			return nil, err
		}
		headers[i] = Header{
			IsNum: isNum,
			Text:  rows[0][i],
		}
	}
	// Make columns
	cols := make([][]string, len(headers))
	for i := range cols {
		cols[i] = make([]string, len(rows)-2)
	}
	for i, row := range rows[2:] {
		for j := range cols {
			cols[j][i] = row[j]
		}
	}
	return &Table{
		Headers: headers,
		Columns: cols,
	}, nil
}

// Apply executes function fn on every table.
func (ts *TableStore) Apply(fn func(*Table)) {
	ids := make(chan string)
	go func() {
		//
		var count int
		//
		defer close(ids)
		dir, err := os.Open(ts.tableDir)
		if err != nil {
			panic(err)
		}
		defer dir.Close()
		var names []string
		for ; err == nil; names, err = dir.Readdirnames(1024) {
			for _, id := range names {
				ids <- id
				//
				count++
				if count == 1000 {
					return
				}
				//
			}
		}
	}()

	for id := range ids {
		t, err := ts.GetTable(id)
		if err != nil {
			log.Printf("Error in reading table %s: %s", id, err)
			continue
		}
		fn(t)
	}
}

// IsNotBuilt checks if the table directory has not been
// created.
func (ts *TableStore) IsNotBuilt() bool {
	_, err := os.Stat(ts.tableDir)
	if os.IsNotExist(err) {
		return true
	}
	if err != nil {
		panic(err)
	}
	return false
}

func (ts *TableStore) getTableFilename(id string) string {
	return filepath.Join(ts.tableDir, id)
}
