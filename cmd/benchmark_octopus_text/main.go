package main

import (
	"log"
	"sync"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	log.Printf("start of octopus text cluster experiments")
	CheckEnv()
	start := GetNow()
	results := make(chan OctopusScore)
	allFilenames := StreamFilenames()
	idf := ComputeIDF(allFilenames)
	queryFilenames := StreamQueryFilenames()
	wg := sync.WaitGroup{}
	go func() {
		for query := range queryFilenames {
			candFilenames := StreamFilenames()
			for i := 0; i < 60; i++ {
				wg.Add(1)
				go func() {
					for candidate := range candFilenames {
						sp := ComputeTextClusterScore(query, candidate, idf)
						results <- sp
					}
					wg.Done()
				}()
			}
			wg.Wait()
		}
		close(results)
	}()
	progress := DoSaveOctopusScores(results)
	total := ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		now := GetNow()
		if total.Values%100 == 0 {
			log.Printf("Processed %d datasets in %.2f seconds\n", total.Values, now-start)
		}
	}
	log.Printf("Done benchmarking Octopus using text cluster score.")
}
