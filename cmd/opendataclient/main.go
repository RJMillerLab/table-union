package main

import (
	"flag"
	"os"

	"github.com/RJMillerLab/table-union/embserver"
	"github.com/RJMillerLab/table-union/table"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var host string
	var queryCSVFilename string
	var resultDir string
	var k int
	var openDataDir string
	var fastTextSqliteDB string
	flag.StringVar(&openDataDir, "opendata-dir", "/home/fnargesian/OPENDATA/datasets",
		"Directory for storing opendata CSV files")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.StringVar(&queryCSVFilename, "query", "",
		"Query CSV file")
	flag.StringVar(&resultDir, "result-dir", "",
		"Query result directory")
	flag.StringVar(&host, "host", "http://localhost:4003", "Server host")
	flag.IntVar(&k, "k", 5, "Top-K")
	flag.Parse()

	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		panic("FastText Sqlite DB does not exist")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)
	ts := table.NewTableStore(openDataDir)

	client, err := embserver.NewClient(ft, ts, host)
	if err != nil {
		panic(err)
	}
	client.Query(queryCSVFilename, k, resultDir)
}
