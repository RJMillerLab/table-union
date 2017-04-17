package table

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
)

type cellRaw struct {
	Text string `json:"text"`
}

type wikiTableRaw struct {
	ID      int         `json:"tableId"`
	Headers [][]Header  `json:"tableHeaders"`
	Rows    [][]cellRaw `json:"tableData"`
}

// Write the wikitable in CSV format.
func (t *Table) WTToCSV(file io.Writer) error {
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

func readRawWT(t wikiTableRaw) (*Table, error) {
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
			cols[j][i] = v
		}
	}
	return &Table{
		Headers: headers,
		Columns: cols,
	}, nil
}

// Build creates the wikitable directory and populate it with
// wikitable CSV files, given the raw wikitable dataset file.
func (ts *TableStore) BuildWT(wikiTableFile io.Reader) error {
	if err := os.MkdirAll(ts.tableDir, 0777); err != nil {
		return err
	}

	tables := make(chan *Table)
	go func() {
		defer close(tables)
		var count int
		scanner := bufio.NewScanner(wikiTableFile)
		scanner.Buffer(make([]byte, bufio.MaxScanTokenSize), bufio.MaxScanTokenSize*256)
		for scanner.Scan() {
			count++
			// Use the order as the table id
			id := strconv.Itoa(count)
			// Skip existing CSV files
			p := ts.getTableFilename(id)
			if _, err := os.Stat(p); err == nil {
				continue
			}
			data := scanner.Bytes()
			var tableRaw wikiTableRaw
			err := json.Unmarshal(data, &tableRaw)
			if err != nil {
				panic(err)
			}
			t, err := readRawWT(tableRaw)
			if err != nil {
				// log.Printf("ReadTable (ID %d): %s", count, err)
				continue
			}
			t.ID = id
			tables <- t
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
	}()

	for table := range tables {
		p := ts.getTableFilename(table.ID)
		f, err := os.Create(p)
		if err != nil {
			return err
		}
		if err := table.WTToCSV(f); err != nil {
			return err
		}
		f.Close()
	}
	return nil
}
