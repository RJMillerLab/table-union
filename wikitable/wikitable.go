package wikitable

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"io"
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

type WikiTable struct {
	ID      int        `json:"id"`
	RawID   int        `json:"raw_id"`
	Headers []Header   `json:"headers"`
	Columns [][]string `json:"columns"`
}

func (t *WikiTable) ToCsv(file io.Writer) error {
	writer := csv.NewWriter(file)
	for i := 0; i < len(t.Columns[0]); i++ {
		row := make([]string, len(t.Headers))
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

func readRaw(t wikiTableRaw) *WikiTable {
	var headers []Header
	numCol := len(t.Rows[0])
	for i := range t.Headers {
		if len(t.Headers[i]) == numCol {
			headers = t.Headers[i]
		}
	}
	if headers == nil {
		panic("Cannot find headers with the same number of fields as row")
	}
	cols := make([][]string, numCol)
	for i := range cols {
		cols[i] = make([]string, len(t.Rows))
	}
	for i := range t.Rows {
		for j := range cols {
			cols[j][i] = t.Rows[i][j].Text
		}
	}
	return &WikiTable{
		RawID:   t.ID,
		Headers: headers,
		Columns: cols,
	}
}

func ReadWikiTable(wikiTableFile io.Reader) chan *WikiTable {
	out := make(chan *WikiTable)
	go func() {
		defer close(out)
		var count int
		scanner := bufio.NewScanner(wikiTableFile)
		for scanner.Scan() {
			data := scanner.Bytes()
			var tableRaw wikiTableRaw
			err := json.Unmarshal(data, &tableRaw)
			if err != nil {
				panic(err)
			}
			if len(tableRaw.Rows) > 0 {
				t := readRaw(tableRaw)
				t.ID = count
				out <- t
				count++
			}
		}
		if err := scanner.Err(); err != nil {
			panic(err)
		}
	}()
	return out
}
