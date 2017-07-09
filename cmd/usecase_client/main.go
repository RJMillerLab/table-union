package main

import (
	"flag"
	"log"
	"os"

	"github.com/RJMillerLab/table-union/experiment"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/unionserver"
	fasttext "github.com/ekzhu/go-fasttext"
)

var (
	FastTextDim = 300
)

func main() {
	var domainDir string
	var queryDir string
	var threshold float64
	var numHash int
	var n int
	var k int
	var port string
	var host string
	var experimentsDB string
	var fastTextSqliteDB string
	var opendataDir string
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/domains",
		"The top-level director for all domain and embedding files")
	flag.IntVar(&numHash, "h", 256, "LSH Parameter: number of hash functions")
	flag.StringVar(&queryDir, "query-dir", "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only",
		"The directory of query files")
	flag.Float64Var(&threshold, "t", 0.6, "Search Parameter: k-unionability threshold")
	// k=5 and n:[1,75]
	flag.IntVar(&k, "k", 5, "Search Parameter: top (n,k) unionable tables")
	flag.IntVar(&n, "n", 10, "Search Parameter: top (n,k) unionable tables")
	flag.StringVar(&host, "host", "http://localhost:4005", "Server host")
	flag.StringVar(&port, "port", "4005", "Server port")
	flag.StringVar(&experimentsDB, "experiments-fixedn-db", "/home/fnargesian/TABLE_UNION_OUTPUT/experiments-fixedn.sqlite", "experiments DB")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.StringVar(&opendataDir, "opendate-dir", "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only", "The directory of open data tables.")
	flag.Parse()
	// Create client
	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		panic("FastText Sqlite DB does not exist")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)

	client, err := unionserver.NewClient(ft, host)
	if err != nil {
		panic(err)
	}
	queries := opendata.StreamQueryFilenames()
	//
	for query := range queries {
		queryPath := experiment.GetQueryPath(queryDir, query)
		log.Printf("Query: %s", queryPath)
		_ = client.QueryAndPreviewResults(queryPath, k, n)
	}
	log.Println("Done experiments.")

}
