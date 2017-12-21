package main

import (
	"log"
	"sync"

	. "github.com/RJMillerLab/table-union/opendata"
)

type pair struct {
	t1name  string
	t2name  string
	colens1 []float64
	colens2 []float64
}

func main() {
	log.Printf("start of octopus size experiments")
	CheckEnv()
	start := GetNow()
	//results := make(chan OctopusScore)
	results := make(chan OctopusAlignment)
	queryFilenames := StreamQueryFilenames()
	pairs := make(chan pair)
	go func() {
		//seen := make(map[string]bool)
		for query := range queryFilenames {
			queryLens := GetTableColumnMeanLength(query)
			candFilenames := StreamFilenames()
			for cand := range candFilenames {
				candLens := GetTableColumnMeanLength(cand)
				//if _, ok := seen[query+" "+cand]; !ok {
				//	if _, ok := seen[cand+" "+query]; !ok {
				//		seen[cand+" "+query] = true
				//		seen[query+" "+cand] = true
				pairs <- pair{
					t1name:  query,
					t2name:  cand,
					colens1: queryLens,
					colens2: candLens,
				}
				//	}
				//}
			}
		}
		close(pairs)
	}()
	wg := sync.WaitGroup{}
	go func() {
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				for p := range pairs {
					//sp := ComputeSizeClusterScore(p.t1name, p.t2name, p.colens1, p.colens2)
					sp := ComputeSizeAlignment(p.t1name, p.t2name, p.colens1, p.colens2)
					results <- sp
				}
				wg.Done()
			}()
		}
		wg.Wait()
		close(results)
	}()
	progress := DoSaveOctopusAlignments(results)
	//progress := DoSaveOctopusScores(results)
	total := ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		now := GetNow()
		if total.Values%100 == 0 {
			log.Printf("Processed %d datasets in %.2f seconds\n", total.Values, now-start)
		}
	}
	log.Printf("Done benchmarking Octopus using size cluster score.")
}
