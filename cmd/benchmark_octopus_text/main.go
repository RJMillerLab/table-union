package main

import (
	"fmt"
	"log"
	"sync"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	start := GetNow()
	results := make(chan OctopusScore)
	allFilenames := StreamQueryFilenames()
	idf := ComputeIDF(allFilenames)
	queryFilenames := StreamQueryFilenames()
	wg := sync.WaitGroup{}
	go func() {
		for query := range queryFilenames {
			candFilenames := StreamQueryFilenames()
			for i := 0; i < 25; i++ {
				wg.Add(1)
				go func() {
					for candidate := range candFilenames {
						log.Printf("%s and %s", query, candidate)
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
			fmt.Printf("Processed %d datasets in %.2f seconds\n", total.Values, now-start)
		}
	}
	fmt.Printf("Done benchmarking octpus")
	log.Printf("Done benchmarking Octopus using text cluster score.")
}
