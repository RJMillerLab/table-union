package main

import (
	"flag"
	"log"
	"sync"

	"github.com/RJMillerLab/table-union/benchmarkserver"
	"github.com/RJMillerLab/table-union/experiment"
	"github.com/RJMillerLab/table-union/opendata"
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
	var fanout int
	var opendataDir string
	var experimentType string
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/domains",
		"The top-level director for all domain and embedding files")
	flag.IntVar(&numHash, "h", 256, "LSH Parameter: number of hash functions")
	flag.StringVar(&queryDir, "query-dir", "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only",
		"The directory of query files")
	flag.Float64Var(&threshold, "t", 0.5, "Search Parameter: k-unionability threshold")
	flag.IntVar(&k, "k", 3, "Search Parameter: top (n,k) unionable tables")
	flag.IntVar(&n, "n", 10, "Search Parameter: top (n,k) unionable tables")
	flag.StringVar(&host, "host", "http://localhost:4007", "Server host")
	flag.StringVar(&port, "port", "4007", "Server port")
	//flag.StringVar(&experimentsDB, "experiments-db", "/home/fnargesian/TABLE_UNION_OUTPUT/jaccard-experiments-fixedn.sqlite", "experiments DB")
	flag.StringVar(&experimentsDB, "experiments-db", "/home/fnargesian/TABLE_UNION_OUTPUT/ont-jaccard-experiments.sqlite", "experiments DB")
	flag.StringVar(&opendataDir, "opendate-dir", "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only", "The    directory of open data tables.")
	flag.IntVar(&fanout, "fanout", 6, "Number threads querying the server in parallel.")
	flag.StringVar(&experimentType, "type", "fixedn", "The type of experiments: fixed k or fixed n.")
	flag.Parse()
	// Create client
	client, err := benchmarkserver.NewOntologyJaccardClient(host, numHash)
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
				if experimentType == "fixedk" {
					results = client.QueryWithFixedK(queryPath, k, n)
				}
				if experimentType == "fixedn" {
					results = client.QueryWithFixedN(queryPath, k, n)
				}
				for _, res := range results {
					log.Printf("results in console")
					res.TableUnion.QueryTableID = query
					alignments <- res.TableUnion
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(alignments)
	}()
	progress := experiment.DoSaveAlignments(alignments, "ontology_"+experimentType, experimentsDB, 1)

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
