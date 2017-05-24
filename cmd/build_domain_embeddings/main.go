package main

import (
	"fmt"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	//ft := fasttext.NewFastText("/home/ekzhu/FB_WORD_VEC/fasttext.db")

	filenames := StreamFilenames()
	valuefreqs := StreamValueFreqFromCache(10, filenames)

	count := 0
	start := GetNow()
	for vf := range valuefreqs {
		count += 1
		_ = vf
		if count%1000 == 0 {
			fmt.Printf("Counted %d domains in %.2f seconds\n", count, GetNow()-start)
		}
	}
	fmt.Printf("Counted %d domains in %.2f seconds\n", count, GetNow()-start)
}
