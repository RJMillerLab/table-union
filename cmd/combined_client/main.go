package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"github.com/RJMillerLab/table-union/benchmarkserver"
	"github.com/RJMillerLab/table-union/experiment"
	"github.com/RJMillerLab/table-union/opendata"
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
	var port string
	var host string
	var experimentsDB string
	var fastTextSqliteDB string
	var fanout int
	var opendataDir string
	var experimentType string
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4/domains",
		"The top-level director for all domain and embedding files")
	flag.IntVar(&numHash, "h", 256, "LSH Parameter: number of hash functions")
	flag.StringVar(&queryDir, "query-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4/csvfiles",
		"The directory of query files")
	flag.Float64Var(&threshold, "t", 0.7, "Search Parameter: k-unionability threshold")
	flag.IntVar(&n, "n", 25, "Search Parameter: top (n,k) unionable tables")
	flag.StringVar(&host, "host", "http://localhost:4075", "Server host")
	flag.StringVar(&port, "port", "4075", "Server port")
	flag.StringVar(&experimentsDB, "experiments-db", "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4/combined.sqlite", "experiments DB")
	flag.StringVar(&opendataDir, "opendate-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4", "The directory of open data tables.")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.IntVar(&fanout, "fanout", 10, "Number threads querying the server in parallel.")
	flag.StringVar(&experimentType, "type", "fixedn", "The type of experiments: fixed k or fixed n.")
	flag.Parse()
	// Create client
	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		panic("FastText Sqlite DB does not exist")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)

	client, err := benchmarkserver.NewCombinedClient(ft, host, numHash)
	if err != nil {
		panic(err)
	}
	queries := opendata.StreamQueryFilenames()
	//
	alignments := make(chan benchmarkserver.Union, 100)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func() {
			for query := range queries {
				queryPath := experiment.GetQueryPath(queryDir, query)
				log.Printf("Query: %s", queryPath)
				results := make([]benchmarkserver.QueryResult, 0)
				results = client.Query(queryPath, n)
				for _, res := range results {
					res.TableUnion.QueryTableID = query
					alignments <- res.TableUnion
				}
			}
			//close(alignments)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(alignments)
	}()
	progress := experiment.DoSaveAlignments(alignments, "scores", experimentsDB, 1)

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
