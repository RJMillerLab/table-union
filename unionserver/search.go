package unionserver

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/simhashlsh"
)

var (
	ByteOrder = binary.BigEndian
)

type UnionIndex struct {
	lsh       *simhashlsh.CosineLSH
	domainDir string
	byteOrder binary.ByteOrder
}

func NewUnionIndex(domainDir string, lsh *simhashlsh.CosineLSH) *UnionIndex {
	index := &UnionIndex{
		lsh:       lsh,
		domainDir: domainDir,
		byteOrder: ByteOrder,
	}
	return index
}

func (index *UnionIndex) Build() error {
	domainfilenames := opendata.StreamFilenames()
	embfilenames := opendata.StreamEmbVectors(10, domainfilenames)
	count := 0
	for file := range embfilenames {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			continue
		}
		count += 1
		if count%100 == 0 {
			log.Printf("indexed %d domains", count)
		}
		vec, err := embedding.ReadVecFromDisk(file, ByteOrder)
		if err != nil {
			log.Printf("Error in reading %s from disk.", file)
			return err
		}
		tableID, columnIndex := parseFilename(index.domainDir, file)
		index.lsh.Add(vec, toColumnID(tableID, columnIndex))
	}
	index.lsh.Index()
	return nil
}

func (index *UnionIndex) Query(query [][]float64, N, K int) <-chan Union {
	start := time.Now()
	results := make(chan Union)
	partialAlign := make(map[string]map[int]bool)
	reverseAlign := make(map[string]map[int]bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		done := make(chan struct{})
		defer close(done)
		aligned := make(map[string]bool)
		for pair := range index.lsh.QueryPlus(query, done) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			if _, ok := partialAlign[tableID]; !ok {
				cols := make(map[int]bool)
				cols[columnIndex] = true
				partialAlign[tableID] = cols
			} else {
				partialAlign[tableID][columnIndex] = true
			}
			if _, ok := reverseAlign[tableID]; !ok {
				cols := make(map[int]bool)
				cols[pair.QueryIndex] = true
				reverseAlign[tableID] = cols
			} else {
				reverseAlign[tableID][pair.QueryIndex] = true
			}

			if len(partialAlign[tableID]) >= K && len(reverseAlign[tableID]) >= K {
				if _, ok := aligned[tableID]; !ok {
					results <- Align(tableID, index.domainDir, query, K)
					aligned[tableID] = true
					log.Printf("Table %s is the %d-th unionable candidate found after %.4f seconds.", tableID, len(aligned), time.Now().Sub(start).Seconds())
				}
				if len(aligned) == N {
					log.Printf("Found %d candidates.", len(aligned))
					wg.Done()
					return
				}
			}
		}
		wg.Done()
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}
