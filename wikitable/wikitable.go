package wikitable

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"unicode"
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
	Headers []Header   `json:"headers"`
	Columns [][]string `json:"columns"`
}

func (t *WikiTable) ToCsv(file io.Writer) error {
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

func FromCsv(file io.Reader) (*WikiTable, error) {
	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		panic("Empty CSV File")
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
			v := t.Rows[i][j].Text
			v = strings.TrimFunc(strings.TrimSpace(v), unicode.IsPunct)
			cols[j][i] = v
		}
	}
	return &WikiTable{
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
		scanner.Buffer(make([]byte, bufio.MaxScanTokenSize), bufio.MaxScanTokenSize*128)
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
