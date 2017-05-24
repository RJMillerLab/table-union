package main

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/RJMillerLab/table-union/embedding"
	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	ft, err := embedding.InitInMemoryFastText("/home/ekzhu/FB_WORD_VEC/fasttext.db", func(v string) []string {
		return strings.Split(v, " ")
	}, func(v string) string {
		return strings.ToLower(strings.TrimFunc(strings.TrimSpace(v), unicode.IsPunct))
	})
	if err != nil {
		panic(err)
	}

	filenames := StreamFilenames()
	valuefreqs := StreamValueFreqFromCache(10, filenames)

	count := 0
	start := GetNow()
	for vf := range valuefreqs {
		vec, err := ft.GetDomainEmbSum(vf.Values, vf.Freq)
		if err != nil {
			fmt.Printf("Error in building embedding for %s - %d: %s\n", vf.Filename, vf.Index, err.Error())
			continue
		}
		vecFilename := filepath.Join(OutputDir, fmt.Sprintf("%s/%d.ft-sum", vf.Filename, vf.Index))
		if err := embedding.WriteVecToDisk(vec, binary.BigEndian, vecFilename); err != nil {
			panic(err)
		}
		count += 1
		if count%10 == 0 {
			fmt.Printf("Counted %d domains in %.2f seconds\n", count, GetNow()-start)
		}
	}
	fmt.Printf("Counted %d domains in %.2f seconds\n", count, GetNow()-start)
}
