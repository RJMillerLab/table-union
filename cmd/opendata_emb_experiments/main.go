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
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/domains",
		"The top-level director for all domain and embedding files")
	flag.IntVar(&numHash, "h", 256, "LSH Parameter: number of hash functions")
	flag.StringVar(&queryDir, "query-dir", "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only",
		"The directory of query files")
	flag.Float64Var(&threshold, "t", 0.8, "Search Parameter: k-unionability threshold")
	flag.IntVar(&k, "k", 5, "Search Parameter: top (n,k) unionable tables")
	flag.IntVar(&n, "n", 10, "Search Parameter: top (n,k) unionable tables")
	flag.StringVar(&host, "host", "http://localhost:4004", "Server host")
	flag.StringVar(&port, "port", "4004", "Server port")
	flag.StringVar(&experimentsDB, "experiments-db", "/home/fnargesian/TABLE_UNION_OUTPUT/experiments.sqlite", "experiments DB")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
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
	alignments := make(chan unionserver.Union)
	go func() {
		for query := range queries {
			queryPath := experiment.GetQueryPath(queryDir, query)
			log.Printf("Query: %s", queryPath)
			results := client.Query(queryPath, k, n)
			for _, res := range results {
				res.TableUnion.QueryTableID = query
				alignments <- res.TableUnion
			}
		}
		close(alignments)
	}()
	progress := experiment.DoSaveAlignments(alignments, "emb_cosine", experimentsDB, 1)

	total := experiment.ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		if total.Values%1 == 0 {
			log.Printf("Calculated and saved %d unionable tables", total.Values)
		}
	}
	log.Printf("Calculated and saved %d unionable tables", total.Values)
	log.Println("Done experiments.")
}
