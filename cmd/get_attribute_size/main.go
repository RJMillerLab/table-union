package main

import (
	"fmt"
	"os"

	. "github.com/RJMillerLab/table-union/opendata"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	CheckEnv()

	start := GetNow()
	filenames := StreamFilenames()
	domains := StreamDomainValuesFromFiles(20, filenames)
	segCount := 0
	totalValueCount := 0
	for domain := range domains {
		sizeFilename := domain.PhysicalFilename("size")
		f, err := os.OpenFile(sizeFilename, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		size := len(domain.Values)
		fmt.Fprintln(f, size)
		f.Close()
	}
	fmt.Printf("%d segments with %d values in %.2f seconds\n", segCount, totalValueCount, GetNow()-start)

}
