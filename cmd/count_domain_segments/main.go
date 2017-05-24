package main

import (
	"fmt"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	count := 0
	start := GetNow()
	for _ = range StreamDomainValuesFromFiles(10, StreamFilenames()) {
		count += 1
		if count%1000 == 0 {
			fmt.Printf("Domain segments: %d in %.2f seconds\n", count, GetNow()-start)
		}
	}
	fmt.Printf("Domain segments: %d in %.2f seconds\n", count, GetNow()-start)
}
