package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"unicode"

	"github.com/RJMillerLab/table-union/embedding"
	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()

	start := GetNow()
	ft, err := embedding.InitInMemoryFastText("/home/ekzhu/FB_WORD_VEC/fasttext.db", func(v string) []string {
		return strings.Split(v, " ")
	}, func(v string) string {
		return strings.ToLower(strings.TrimFunc(strings.TrimSpace(v), unicode.IsPunct))
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("fasttext.db loaded in %.2f seconds.\n", GetNow()-start)

	filenames := StreamFilenames()
	valuefreqs := StreamValueFreqFromCache(10, filenames)

	progress := make(chan ProgressCounter)
	fanout := 40
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func() {
			for vf := range valuefreqs {
				// calculating mean
				//vec, err := ft.GetDomainEmbSum(vf.Values, vf.Freq)
				log.Printf("file: %s", vf.Filename)
				//mean, covar, err := ft.GetDomainEmbMeanCovar(vf.Values, vf.Freq)
				mean, covar, err := ft.GetDomainEmbMeanVar(vf.Values, vf.Freq)
				if err != nil {
					fmt.Printf("Error in building embedding for %s - %d: %s\n", vf.Filename, vf.Index, err.Error())
					continue
				}
				//vecFilename := filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-sum", vf.Filename, vf.Index))
				vecFilename := filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-mean", vf.Filename, vf.Index))
				if err := embedding.WriteVecToDisk(mean, binary.BigEndian, vecFilename); err != nil {
					panic(err)
				}
				vecFilename = filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-covar", vf.Filename, vf.Index))
				if err := embedding.WriteVecToDisk(covar, binary.BigEndian, vecFilename); err != nil {
					panic(err)
				}
				log.Printf("embeddig %s", vf.Filename)
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(progress)
	}()
	i := 0
	total := ProgressCounter{}
	for n := range progress {
		total.Values += n.Values
		i += 1
		log.Printf("Processed %d domains.", i, total.Values)
	}
	log.Printf("Finished counting %d domains.", total.Values)
}
