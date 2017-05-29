package main

import (
	"flag"
	"os"

	"github.com/RJMillerLab/table-union/embserver"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var domainDir string
	var host string
	var queryCSVFilename string
	var k int
	var fastTextSqliteDB string
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/domains",
		"The top-level director for all domain and embedding files")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.StringVar(&queryCSVFilename, "query", "",
		"Query CSV file")
	flag.StringVar(&host, "host", "http://localhost:4003", "Server host")
	flag.IntVar(&k, "k", 5, "Top-K")
	flag.Parse()

	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		panic("FastText Sqlite DB does not exist")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)

	client, err := embserver.NewClient(ft, host)
	if err != nil {
		panic(err)
	}
	_ = client.Query(queryCSVFilename, k)
}
