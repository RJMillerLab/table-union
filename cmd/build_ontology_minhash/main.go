package main

import (
	"fmt"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	start := GetNow()
	filenames := StreamFilenames()
	sketches := DoOntologyMinhashFromDB(10, filenames)
	progress := DoSaveDomainSketches(10, sketches, "ont-minhash-l1")
	total := ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		now := GetNow()
		if total.Values%100 == 0 {
			fmt.Printf("Processed %d domains in %.2f seconds\n", total.Values, now-start)
		}
	}
	fmt.Printf("Done generating l2 minhash sketches for COD.")
}
