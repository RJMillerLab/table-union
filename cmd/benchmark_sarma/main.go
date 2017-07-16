package main

import (
	"fmt"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	InitSarma()
	start := GetNow()
	queryFilenames := StreamQueryFilenames()
	results := DoFindSarmaUnionableTables(queryFilenames, 2)
	progress := DoSaveSarmaScores(results)
	total := ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		now := GetNow()
		if total.Values%100 == 0 {
			fmt.Printf("Processed %d datasets in %.2f seconds\n", total.Values, now-start)
		}
	}
	fmt.Printf("Done benchmarking sarma")
}
