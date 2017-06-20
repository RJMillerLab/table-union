package main

import (
	"fmt"
	"log"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	start := GetNow()
	total := ProgressCounter{}
	queries := GetQueryFilenames()
	log.Printf("number of queries: %d", len(queries))
	filenames := GetCODFilenames()
	log.Printf("number of CODs: %d", len(filenames))
	scores := DoAlign(queries, filenames, 40)
	progress := DoSaveKUnionabilityScores(scores, 1)
	for n := range progress {
		total.Values += n.Values
		now := GetNow()
		if total.Values%100 == 0 {
			fmt.Printf("Calculated unionability scores for %d table pairs in %.2f seconds\n", total.Values, now-start)
		}
	}
	now := GetNow()
	fmt.Printf("Calculated unionability scores for %d table pairs in %.2f seconds\n", total.Values, now-start)
	fmt.Println("Done calculating scores.")
}
