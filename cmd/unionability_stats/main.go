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
	colens1 []float64
	colens2 []float64
}

func main() {
	CheckEnv()
	tstart := GetNow()
	astart := tstart
	queryFilenames := StreamQueryFilenames()
	attProgress := make(chan ProgressCounter)
	tableProgress := make(chan ProgressCounter)
	allAttUnions := make(chan []AttributeUnion)
	allTableUnions := make(chan TableUnion)
	go func() {
		seen := make(map[string]bool)
		for query := range queryFilenames {
			candFilenames := StreamQueryFilenames()
			wg := &sync.WaitGroup{}
			for i := 0; i < 30; i++ {
				wg.Add(1)
				go func() {
					for cand := range candFilenames {
						if _, ok := seen[query+" "+cand]; !ok {
							if _, ok := seen[cand+" "+query]; !ok {
								seen[cand+" "+query] = true
								seen[query+" "+cand] = true
								attUnions, qColNum, cColNm := ComputeAttUnionabilityScores(query, cand)
								allAttUnions <- attUnions
								tableUnion := ComputeTableUnionability(query, cand, attUnions, qColNum, cColNm)
								allTableUnions <- tableUnion
							}
						}
					}
					wg.Done()
				}()
			}
			wg.Wait()
		}
	}()
	swg := &sync.WaitGroup{}
	swg.Add(4)
	go func() {
		for attUnions := range allAttUnions {
			DoSaveAttScores(attUnions, attProgress)
		}
		close(attProgress)
		swg.Done()
	}()
	go func() {
		for tableUnion := range allTableUnions {
			DoSaveTableScores(tableUnion, tableProgress)
		}
		close(tableProgress)
		swg.Done()
	}()
	go func() {
		total := ProgressCounter{}
		for n := range attProgress {
			total.Values += n.Values
			now := GetNow()
			if total.Values%100 == 0 {
				fmt.Printf("Processed %d attributes in %.2f seconds\n", total.Values, now-astart)
			}
		}
	}()
	go func() {
		total := ProgressCounter{}
		for n := range tableProgress {
			total.Values += n.Values
			now := GetNow()
			if total.Values%100 == 0 {
				fmt.Printf("Processed %d tables in %.2f seconds\n", total.Values, now-tstart)
			}
		}
	}()
	swg.Wait()

	log.Printf("Done collecting stats.")
}
