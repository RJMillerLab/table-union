package main

import (
	"fmt"
	. "opendata"
)

const nReaders = 10
const nWriters = 10

func main() {
	CheckEnv()

	filenames := StreamFilenames()
	domains := StreamDomainsFromFilenames(nReaders, filenames)
	progress := DoSaveDomainValues(nWriters, domains)

	i := 0
	total := ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		i += 1

		if i%1000 == 0 {
			fmt.Printf("%d fragments, with %d values\n", i, total.Values)
		}
	}

	fmt.Println("Done")
}
