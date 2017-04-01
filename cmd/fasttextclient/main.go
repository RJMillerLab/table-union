package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/RJMillerLab/fastTextHomeWork/server"
	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	"github.com/ekzhu/datatable"
)

func main() {
	var host string
	var queryCSVFilename string
	var resultDir string
	var k int
	var wikiTableDir string
	flag.StringVar(&wikiTableDir, "wikitable-dir", "/home/ekzhu/WIKI_TABLE/tables",
		"Directory for storing wikitable CSV files")
	flag.StringVar(&queryCSVFilename, "query", "",
		"Query CSV file")
	flag.StringVar(&resultDir, "result-dir", "",
		"Query result directory")
	flag.StringVar(&host, "host", "http://localhost:4003", "Server host")
	flag.IntVar(&k, "k", 5, "Top-K")
	flag.Parse()

	// Check resource
	ts := wikitable.NewWikiTableStore(wikiTableDir)
	if ts.IsNotBuilt() {
		panic("WikiTable directory is not built")
	}

	// Make query
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	headers, err := reader.Read()
	if err != nil {
		panic(err)
	}
	queryTable, err := datatable.FromCSV(reader)
	if err != nil {
		panic(err)
	}
	request := server.QueryRequest{
		Table: queryTable,
		K:     k,
	}
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(&request); err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", host+"/query", buf)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "/application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	queryResponseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var queryResponse server.QueryResponse
	if err := json.Unmarshal(queryResponseData, &queryResponse); err != nil {
		panic(err)
	}

	// Create results
	if err := os.MkdirAll(resultDir, 0777); err != nil {
		panic(err)
	}
	for i, result := range queryResponse.Results {
		if result == nil || len(result) == 0 {
			log.Printf("No result for column %d (%s)", i, headers[i])
			continue
		}
		log.Printf("Result for column %d (%s)", i, headers[i])
		colResultDir := filepath.Join(resultDir, fmt.Sprintf("c%d_(%s)", i, headers[i]))
		if err := os.MkdirAll(colResultDir, 0777); err != nil {
			panic(err)
		}
		for i, v := range result {
			log.Printf("(Rank %d) Table %s, Column %d", i, v.TableID, v.ColumnIndex)
			t, err := ts.GetTable(v.TableID)
			outputFilename := filepath.Join(colResultDir,
				fmt.Sprintf("(Rank %d)_%s_c%d_(%s).csv",
					i,
					v.TableID,
					v.ColumnIndex,
					url.PathEscape(t.Headers[v.ColumnIndex].Text)))
			f, err := os.Create(outputFilename)
			if err != nil {
				panic(err)
			}
			if err := t.ToCSV(f); err != nil {
				panic(err)
			}
			f.Close()
		}
	}
}