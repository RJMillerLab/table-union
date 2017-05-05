package main

import (
	"fmt"
	. "od"
)

const nReaders = 2
const nWriters = 2

func main() {
	CheckEnv()

	progress := make(chan ProgressCounter)

	datafiles := GetDatafileChan()
	domains := GetDomains(datafiles, nReaders)
	done := SaveDomainValues(domains, nWriters, progress)

	go func() {
		total := &ProgressCounter{}
		start := GetNow()
		for n := range progress {
			total.Domains += n.Domains
			total.Values += n.Values
			fmt.Printf("Saved %d domains, %d values, took \"%.2f\" seconds\n", total.Domains, total.Values, GetNow()-start)
		}
	}()

	<-done
	close(progress)
}
