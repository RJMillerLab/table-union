package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/RJMillerLab/fastTextHomeWork/embserver"
	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var host string
	var queryCSVFilename string
	var resultDir string
	var k int
	var benchmarkDir string
	var fastTextSqliteDB string
	var batchQueryFilename string
	flag.StringVar(&benchmarkDir, "benchmark-dir", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/testdata", "Directory when benchmark CSV files are stored.")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.StringVar(&queryCSVFilename, "query", "",
		"Query CSV file")
	flag.StringVar(&resultDir, "result-dir", "",
		"Query result directory")
	flag.StringVar(&batchQueryFilename, "batch-query", "", "The file containing a list of queries.")
	flag.StringVar(&host, "host", "http://localhost:4004", "Server host")
	flag.IntVar(&k, "k", 5, "Top-K")
	flag.Parse()

	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		panic("FastText Sqlite DB does not exist")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)
	ts := wikitable.NewWikiTableStore(benchmarkDir)

	client, err := embserver.NewClient(ft, ts, host)
	if err != nil {
		panic(err)
	}
	if batchQueryFilename != "" {
		f, err := os.Open(batchQueryFilename)
		if err != nil {
			fmt.Printf("Query file panic")
			panic(err.Error())
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			queryCSVFilename := scanner.Text()
			qname := queryCSVFilename[strings.LastIndex(queryCSVFilename, "/")+1:]
			client.Query(queryCSVFilename, k, filepath.Join(resultDir, qname))
		}
	}
	if queryCSVFilename != "" {
		client.Query(queryCSVFilename, k, resultDir)
	}
}

func evaluate() {

}
