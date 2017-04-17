package table

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

type ontCellRaw struct {
	Entity string `json:"tdHtmlString"`
}

type ontWikiTableRaw struct {
	ID      int            `json:"tableId"`
	Headers [][]Header     `json:"tableHeaders"`
	Rows    [][]ontCellRaw `json:"tableData"`
}

func readRawOnt(t ontWikiTableRaw) (*Table, error) {
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
			// extract the entity correspondinf to a cell from html
			v := cellToEntity(t.Rows[i][j].Entity)
			cols[j][i] = v
		}
	}
	return &Table{
		Headers: headers,
		Columns: cols,
	}, nil
}

// OntBuild creates the wikitable directory and populate it with
// wikitable CSV files where cell values are entities corresponding to raw text.
func (ts *TableStore) OntBuild(wikiTableFile io.Reader) error {
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
			var tableRaw ontWikiTableRaw
			err := json.Unmarshal(data, &tableRaw)
			if err != nil {
				panic(err)
			}
			t, err := readRawOnt(tableRaw)
			if err != nil {
				log.Printf("ReadWikiTable (ID %d): %s", count, err)
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

// cellToEntity extracts the entity name from html tag
// example:
//"tdHtmlString" : "<td colspan=\"1\" rowspan=\"5\"><a href=\"http://www.wikipedia.org/wiki/Football_League_First_Division\" shape=\"rect\">Division One</a> </td>"
func cellToEntity(htmlCell string) string {
	ahinx := strings.Index(htmlCell, "a href=\"http://www.wikipedia.org/wiki/")
	c := ""
	if ahinx != -1 {
		c = htmlCell[strings.Index(htmlCell, "a href=\"http://www.wikipedia.org/wiki/"):]
		c = strings.Replace(c, "a href=\"http://www.wikipedia.org/wiki/", "", 1)
		c = c[:strings.Index(c, "\"")]
	}
	return c
}
