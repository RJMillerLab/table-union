package main

import (
	"fmt"

	. "github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/yago"
)

func main() {
	CheckEnv()
	InitSarma()
	start := GetNow()
	yg := yago.InitYago(Yago_db)
	queryFilenames := StreamQueryFilenames()
	results := DoFindSarmaUnionableTables(queryFilenames, 3, yg)
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
