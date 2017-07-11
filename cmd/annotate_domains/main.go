package main

import (
	"fmt"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	start := GetNow()
	InitAnnotator()
	filenames := StreamFilenames()
	annotations := AnnotateDomainsFromEntityFiles(filenames, 30, "entities-l0")
	progress := DoSaveAnnotations(annotations)
	total := ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		now := GetNow()
		if total.Values%100 == 0 {
			fmt.Printf("Processed %d subject columns in %.2f seconds\n", total.Values, now-start)
		}
	}
	fmt.Printf("Done annotations for subject columns for COD.")
}
