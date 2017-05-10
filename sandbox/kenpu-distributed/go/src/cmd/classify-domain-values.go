package main

import (
	"fmt"
	. "opendata"
)

func main() {
	CheckEnv()

	filenames := StreamFilenames()
	progress := DoClassifyDomainsFromFiles(1, filenames)

	start := GetNow()
	tick := start
	total := 0
	for n := range progress {
		total += n
		now := GetNow()

		if now-tick > 10 {
			tick = now
			fmt.Printf("Classified %d data files in %.2f seconds\n", total, GetNow()-start)
		}
	}
}
