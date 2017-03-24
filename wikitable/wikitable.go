package wikitable

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"
)

var (
	ErrEmptyTable   = errors.New("Empty table")
	ErrNoTableFound = errors.New("No such table found")
)

type cellRaw struct {
	Text string `json:"text"`
}

type wikiTableRaw struct {
	ID      int         `json:"tableId"`
	Headers [][]Header  `json:"tableHeaders"`
	Rows    [][]cellRaw `json:"tableData"`
}

type Header struct {
	IsNum bool   `json:"isNumeric"`
	Text  string `json:"text"`
}

// WikiTable is a in-memory representation of a wikitable.
type WikiTable struct {
	ID      int        `json:"id"`
	Headers []Header   `json:"headers"`
	Columns [][]string `json:"columns"`
}

// Write the wikitable in CSV format.
func (t *WikiTable) ToCsv(file io.Writer) error {
	if len(t.Columns) == 0 {
		return errors.New("Empty table")
	}
	writer := csv.NewWriter(file)
	row := make([]string, len(t.Headers))
	// Write headers
	for i := range row {
		row[i] = t.Headers[i].Text
	}
	if err := writer.Write(row); err != nil {
		return err
	}
	// Write column types
	for i := range row {
		row[i] = strconv.FormatBool(t.Headers[i].IsNum)
	}
	if err := writer.Write(row); err != nil {
		return err
	}
	// Write rows
	for i := 0; i < len(t.Columns[0]); i++ {
		for j := range row {
			row[j] = t.Columns[j][i]
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	return nil
}

// Converts a CSV file to wikitable.
func FromCsv(file io.Reader) (*WikiTable, error) {
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
		cols[i] = make([]string, len(rows))
	}
	for i := range rows {
		for j := range cols {
			cols[j][i] = rows[i][j]
		}
	}
	return &WikiTable{
		Headers: headers,
		Columns: cols,
	}, nil
}

func readRaw(t wikiTableRaw) (*WikiTable, error) {
	if len(t.Rows) <= 0 {
		return nil, errors.New("Raw table has no row")
	}
	var headers []Header
	numCol := len(t.Rows[0])
	if numCol == 0 {
		return nil, errors.New("Raw table has no column")
	}
	for i := range t.Headers {
		if len(t.Headers[i]) == numCol {
			headers = t.Headers[i]
		}
	}
	if headers == nil {
		return nil, errors.New("Cannot find headers with the same number of fields as row")
	}
	// Transpose
	cols := make([][]string, numCol)
	for i := range cols {
		cols[i] = make([]string, len(t.Rows))
	}
	for i := range t.Rows {
		for j := range cols {
			v := t.Rows[i][j].Text
			v = strings.TrimFunc(strings.TrimSpace(v), unicode.IsPunct)
			cols[j][i] = v
		}
	}
	return &WikiTable{
		Headers: headers,
		Columns: cols,
	}, nil
}

// ReadWikiTable provides a channel reader for the raw wikitable dataset.
func ReadWikiTable(wikiTableFile io.Reader) chan *WikiTable {
	out := make(chan *WikiTable)
	go func() {
		defer close(out)
		var count int
		scanner := bufio.NewScanner(wikiTableFile)
		scanner.Buffer(make([]byte, bufio.MaxScanTokenSize), bufio.MaxScanTokenSize*128)
		for scanner.Scan() {
			data := scanner.Bytes()
			var tableRaw wikiTableRaw
			err := json.Unmarshal(data, &tableRaw)
			if err != nil {
				panic(err)
			}
			t, err := readRaw(tableRaw)
			count++
			if err != nil {
				log.Printf("ReadWikiTable (ID %d): %s", count, err)
				continue
			}
			t.ID = count
			out <- t
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
	}()
	return out
}

// WikiTableStore is provides an interface for the file system
// directory storing the wikitable CSV files.
type WikiTableStore struct {
	wikiTableDir string
}

func NewWikiTableStore(wikiTableDir string) *WikiTableStore {
	return &WikiTableStore{
		wikiTableDir: wikiTableDir,
	}
}

// GetTable gets a wikitable given its ID.
func (ts *WikiTableStore) GetTable(id int) (*WikiTable, error) {
	p := filepath.Join(ts.wikiTableDir, strconv.Itoa(id))
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, ErrNoTableFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t, err := FromCsv(f)
	if err != nil {
		return nil, err
	}
	t.ID = id
	return t, nil
}

// Apply executes function fn on every wikitable.
func (ts *WikiTableStore) Apply(fn func(*WikiTable)) {
	ids := make(chan int)
	go func() {
		defer close(ids)
		dir, err := os.Open(ts.wikiTableDir)
		if err != nil {
			panic(err)
		}
		defer dir.Close()
		var names []string
		for ; err == nil; names, err = dir.Readdirnames(1024) {
			for _, n := range names {
				id, err := strconv.Atoi(n)
				if err != nil {
					panic(err)
				}
				ids <- id
			}
		}
	}()

	for id := range ids {
		t, err := ts.GetTable(id)
		if err != nil {
			panic(err)
		}
		fn(t)
	}
}

// IsNotBuilt checks if the wikitable directory has not been
// created.
func (ts *WikiTableStore) IsNotBuilt() bool {
	_, err := os.Stat(ts.wikiTableDir)
	if os.IsNotExist(err) {
		return true
	}
	if err != nil {
		panic(err)
	}
	return false
}

// Build creates the wikitable directory and populate it with
// wikitable CSV files, given the raw wikitable dataset file.
func (ts *WikiTableStore) Build(wikiTableFile io.Reader) error {
	if err := os.MkdirAll(ts.wikiTableDir, 0777); err != nil {
		return err
	}
	for table := range ReadWikiTable(wikiTableFile) {
		p := filepath.Join(ts.wikiTableDir, strconv.Itoa(table.ID))
		f, err := os.Create(p)
		if err != nil {
			return err
		}
		if err := table.ToCsv(f); err != nil {
			return err
		}
		f.Close()
	}
	return nil
}
