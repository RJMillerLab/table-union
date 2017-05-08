package main

import (
	"fmt"
	. "opendata"
)

const nReaders = 30
const nWriters = 30

func main() {
	CheckEnv()

	filenames := StreamFilenames()
	domains := StreamDomainsFromFilenames(nReaders, filenames)
	progress := DoSaveDomainValues(nWriters, domains)

	i := 0
	total := ProgressCounter{}
	start := GetNow()
	tick := GetNow()
	for n := range progress {
		total.Values += n.Values
		i += 1
		now := GetNow()

		if now-tick > 10 {
			tick = now
			fmt.Printf("[fragment %d] written %d values in %.2f seconds\n", i, total.Values, now-start)
		}
	}

	fmt.Printf("Done, written %d values in %.2f seconds\n", total.Values, GetNow()-start)
}
