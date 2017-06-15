package main

import (
	"flag"
	"os"

	"github.com/RJMillerLab/table-union/unionserver"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var domainDir string
	var host string
	var queryCSVFilename string
	var n, k int
	var fastTextSqliteDB string
	var resultDir string
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/domains",
		"The top-level director for all domain and embedding files")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.StringVar(&queryCSVFilename, "query", "",
		"Query CSV file")
	flag.StringVar(&resultDir, "result-dir", "",
		"Query result directory")
	flag.StringVar(&host, "host", "http://localhost:4004", "Server host")
	flag.IntVar(&n, "n", 10, "Search Parameter: top (n,k) unionable tables")
	flag.IntVar(&k, "k", 1, "Search Parameter: top (n,k) unionable tables")
	flag.Parse()

	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		panic("FastText Sqlite DB does not exist")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)

	client, err := unionserver.NewClient(ft, host, domainDir)
	if err != nil {
		panic(err)
	}
	_ = client.Query(queryCSVFilename, k, n)
}
