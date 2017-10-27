package main

import (
	"fmt"
	"log"
	"sync"

	. "github.com/RJMillerLab/table-union/opendata"
)

type pair struct {
	t1name  string
	t2name  string
	tfidfs1 []map[string]float64
	tfidfs2 []map[string]float64
	l2s1    []float64
	l2s2    []float64
}

func main() {
	CheckEnv()
	start := GetNow()
	results := make(chan OctopusScore)
	allFilenames := StreamQueryFilenames()
	idf := ComputeIDF(allFilenames)
	queryFilenames := StreamQueryFilenames()
	pairs := make(chan pair)
	go func() {
		seen := make(map[string]bool)
		for query := range queryFilenames {
			queryTfidfs, queryL2s := GetTableColumnsTFIDF(query, idf)
			candFilenames := StreamQueryFilenames()
			for cand := range candFilenames {
				candTfidfs, candL2s := GetTableColumnsTFIDF(cand, idf)
				if _, ok := seen[query+" "+cand]; !ok {
					if _, ok := seen[cand+" "+query]; !ok {
						seen[cand+" "+query] = true
						seen[query+" "+cand] = true
						pairs <- pair{
							t1name:  query,
							t2name:  cand,
							tfidfs1: queryTfidfs,
							tfidfs2: candTfidfs,
							l2s1:    queryL2s,
							l2s2:    candL2s,
						}
					}
				}
			}
		}
		close(pairs)
	}()
	wg := sync.WaitGroup{}
	go func() {
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func() {
				for p := range pairs {
					sp := ComputeColumnTextClusterScore(p.t1name, p.t2name, p.tfidfs1, p.tfidfs2, p.l2s1, p.l2s2)
					results <- sp
				}
				wg.Done()
			}()
		}
		wg.Wait()
		close(results)
	}()
	progress := DoSaveOctopusScores(results)
	total := ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		now := GetNow()
		if total.Values%100 == 0 {
			fmt.Printf("Processed %d datasets in %.2f seconds\n", total.Values, now-start)
		}
	}
	log.Printf("Done benchmarking Octopus using size cluster score.")
}
