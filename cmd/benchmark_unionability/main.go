package main

import (
	"fmt"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	start := GetNow()
	total := ProgressCounter{}
	// load ontology
	Init()
	//read queries
	queries := StreamFilenames()
	for query := range queries {
		filenames := StreamFilenames()
		scores := DoComputeUnionability(query, filenames, 30)
		progress := DoSaveScores(scores, SarmaFilename, JaccardFilename, ContainmentFilename, CosineFilename, 5)
		for n := range progress {
			total.Values += n.Values
			now := GetNow()
			if total.Values%100 == 0 {
				fmt.Printf("Calculated unionability scores for %d domains in %.2f seconds\n", total.Values, now-start)
			}
		}
	}
	now := GetNow()
	fmt.Printf("Calculated unionability scores for %d domains in %.2f seconds\n", total.Values, now-start)
	fmt.Println("Done calculating scores.")
}
