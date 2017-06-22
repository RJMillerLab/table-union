package main

import (
	"flag"
	"log"

	"github.com/RJMillerLab/table-union/experiment"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/unionserver"
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
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/domains",
		"The top-level director for all domain and embedding files")
	flag.IntVar(&numHash, "h", 256, "LSH Parameter: number of hash functions")
	flag.StringVar(&queryDir, "query-dir", "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only",
		"The directory of query files")
	flag.Float64Var(&threshold, "t", 0.8, "Search Parameter: k-unionability threshold")
	flag.IntVar(&k, "k", 5, "Search Parameter: top (n,k) unionable tables")
	flag.IntVar(&n, "n", 10, "Search Parameter: top (n,k) unionable tables")
	flag.StringVar(&host, "host", "http://localhost:4003", "Server host")
	flag.StringVar(&port, "port", "4003", "Server port")
	flag.StringVar(&experimentsDB, "experiments-db", "/home/fnargesian/TABLE_UNION_OUTPUT/experiments.sqlite", "experiments DB")
	flag.Parse()
	// Create client
	client, err := unionserver.NewJaccardClient(host, numHash)
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
	progress := experiment.DoSaveAlignments(alignments, "simhash_jaccard", experimentsDB, 1)

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
