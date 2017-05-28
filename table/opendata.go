package table

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
)

var (
	NumRow = 10
)

type openDatasetRaw struct {
	ID       string
	Headers  []Header
	Filename string
}

// Write the opendataset in CSV format.
func (od *openDatasetRaw) ODToCSV(file io.Writer) error {
	if len(od.Headers) == 0 {
		return errors.New("Empty table")
	}
	writer := csv.NewWriter(file)
	row := make([]string, len(od.Headers))
	// Write headers
	for i := range row {
		row[i] = od.Headers[i].Text
	}
	if err := writer.Write(row); err != nil {
		return err
	}
	// Write column types
	for i := range row {
		row[i] = strconv.FormatBool(od.Headers[i].IsNum)
	}
	if err := writer.Write(row); err != nil {
		return err
	}
	// Write rows
	errc := make(chan error, 1)
	ch := make(chan []string, 10)
	go func() {
		defer close(errc)
		f, err := os.Open(od.Filename)
		if err != nil {
			errc <- err
			return
		}
		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			errc <- err
			return
		}
		defer close(ch)
		for {
			rec, err := r.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				errc <- err
				return
			}
			ch <- rec
		}
	}()
	for row := range ch {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return err
	}
	if err := <-errc; err != nil {
		return err
	}
	return nil
}

// BuildOD creates the opendata directory and populate it with
// opendata CSV files, given the raw opendata dataset file.
func (ods *TableStore) BuildOD(openDataFile io.Reader) error {
	if err := os.MkdirAll(ods.tableDir, 0777); err != nil {
		return err
	}
	datasets := make(chan *openDatasetRaw)
	go func() {
		defer close(datasets)
		var count int

		r := csv.NewReader(openDataFile)
		for {
			count++
			// Use the order as the table id
			id := strconv.Itoa(count)
			// Skip existing CSV files
			p := ods.getTableFilename(id)
			if _, err := os.Stat(p); err == nil {
				continue
			}
			odf, rerr := r.Read()
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				log.Fatal(rerr)
			}
			odr, err := readRawOD(odf[0])
			if err != nil {
				continue
			}
			odr.ID = id
			datasets <- odr
		}

	}()

	for dataset := range datasets {
		p := ods.getTableFilename(dataset.ID)
		f, err := os.Create(p)
		if err != nil {
			return err
		}
		if err := dataset.ODToCSV(f); err != nil {
			return err
		}
		f.Close()
	}
	return nil
}

func readRawOD(datasetFileName string) (*openDatasetRaw, error) {
	f, err := os.Open(datasetFileName)
	if err != nil {
		return nil, err
	}
	var headers []Header
	r := csv.NewReader(f)
	//read header
	rec, err := r.Read()
	if err != nil {
		return nil, err
	}
	for _, v := range rec {
		var h Header
		h.Text = v
		headers = append(headers, h)
	}
	for i := 0; i < 10; i++ {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for j, v := range rec {
			if isNumeric(v) == true {
				headers[j].IsNum = true
			}
		}
	}
	return &openDatasetRaw{
		Headers:  headers,
		Filename: datasetFileName,
	}, nil
}

func isNumeric(cell string) bool {
	if _, err := strconv.ParseFloat(cell, 64); err != nil {
		return false
	}
	return true
}
