package main

import (
	"fmt"
	. "opendata"
)

func main() {
	CheckEnv()

	filenames := StreamFilenames()
	progress := DoClassifyDomainsFromFiles(10, filenames)

	s := GetNow()
	total := 0
	for n := range progress {
		total += n
		fmt.Printf("Classified %d data files in %.2f seconds\n", total, GetNow()-s)
	}
}
