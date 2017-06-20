package unionserver

import (
	"encoding/binary"
	"log"
	"os"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/pqueue"
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
		if count%1000 == 0 {
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

func (index *UnionIndex) QueryOrderAll(query [][]float64, N, K int) <-chan Union {
	log.Printf("Started querying the index with %d columns.", len(query))
	//start := time.Now()
	results := make(chan Union)
	partialAlign := make(map[string]map[int]bool)
	reverseAlign := make(map[string]map[int]bool)
	tableQueues := make(map[string]*pqueue.TopKQueue)
	aligneQueue := pqueue.NewTopKQueue(2000)
	count := 0
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		done := make(chan struct{})
		defer close(done)
		aligned := make(map[string]bool)
		for pair := range index.lsh.QueryPlus(query, done) {
			count += 1
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			// discard columns of already aligned tables
			if _, ok := aligned[tableID]; !ok {
				// getting the embedding of the candidate column
				embFilename := getEmbFilename(tableID, index.domainDir, columnIndex)
				if _, err := os.Stat(embFilename); os.IsNotExist(err) {
					log.Printf("Embedding file %s does not exist.", embFilename)
					continue
				}
				vec, err := embedding.ReadVecFromDisk(embFilename, ByteOrder)
				if err != nil {
					log.Printf("Error in reading %s from disk.", embFilename)
					continue
				}
				// inserting the pair into its corresponding priority queue
				cosine := embedding.Cosine(vec, query[pair.QueryIndex])
				e := Pair{
					QueryColIndex: pair.QueryIndex,
					CandTableID:   tableID,
					CandColIndex:  columnIndex,
					Sim:           cosine,
				}
				aligneQueue.Push(e, -1*cosine)
				if aligneQueue.Size() == 2000 {
					entry, cosine := aligneQueue.Pop()
					pair := entry.(Pair)
					if _, ok := tableQueues[pair.CandTableID]; !ok {
						pq := pqueue.NewTopKQueue(K)
						pq.Push(pair, cosine)
						tableQueues[pair.CandTableID] = pq
						cols1 := make(map[int]bool)
						cols1[pair.CandColIndex] = true
						partialAlign[pair.CandTableID] = cols1
						cols2 := make(map[int]bool)
						cols2[pair.QueryColIndex] = true
						reverseAlign[pair.CandTableID] = cols2
					} else {
						if _, ok := partialAlign[pair.CandTableID][pair.CandColIndex]; !ok {
							if _, ok := reverseAlign[pair.CandTableID][pair.QueryColIndex]; !ok {
								partialAlign[pair.CandTableID][pair.CandColIndex] = true
								reverseAlign[pair.CandTableID][pair.QueryColIndex] = true
								pq := tableQueues[pair.CandTableID]
								pq.Push(pair, cosine)
								tableQueues[pair.CandTableID] = pq
								if tableQueues[pair.CandTableID].Size() == K {
									aligned[pair.CandTableID] = true
									results <- AlignTooEasy(tableQueues[pair.CandTableID], index.domainDir)
									if len(aligned) == N {
										log.Printf("Number of received pairs is %d", count)
										wg.Done()
										return
									}
								}
							}
						}
					}
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
