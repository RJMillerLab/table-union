package unionserver

import (
	"encoding/binary"
	"log"
	"os"

	"github.com/ekzhu/counter"
	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/pqueue"
	"github.com/RJMillerLab/table-union/simhashlsh"
)

var (
	ByteOrder = binary.BigEndian
	batchSize = 2000
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

type alignment struct {
	completedTables *counter.Counter
	partialAlign    map[string](*counter.Counter)
	reverseAlign    map[string](*counter.Counter)
	tableQueues     map[string](*pqueue.TopKQueue)
	k               int
	n               int
}

func initAlignment(K, N int) alignment {
	return alignment{
		completedTables: counter.NewCounter(),
		partialAlign:    make(map[string](*counter.Counter)),
		reverseAlign:    make(map[string](*counter.Counter)),
		tableQueues:     make(map[string]*pqueue.TopKQueue),
		k:               K,
		n:               N,
	}
}

func (a alignment) hasCompleted(tableID string) bool {
	return a.completedTables.Has(tableID)
}

func (a alignment) hasPartialTable(tableID string) bool {
	_, has := a.partialAlign[tableID]
	return has
}

func (a alignment) hasSeenBetter(pair Pair) bool {
	if !a.hasPartialTable(pair.CandTableID) {
		return false
	}
	return a.partialAlign[pair.CandTableID].Has(pair.CandColIndex) &&
		a.reverseAlign[pair.CandTableID].Has(pair.QueryColIndex)
}

// Produces an alignment
func (a alignment) get(candidateTableID string) []Pair {
	if !a.hasCompleted(candidateTableID) {
		panic("This table has not been completed")
	}
	ps, _ := a.tableQueues[candidateTableID].Descending()
	pairs := make([]Pair, len(ps))
	for i := range pairs {
		pairs[i] = ps[i].(Pair)
	}
	return pairs
}

func (a alignment) processPairs(pairQueue *pqueue.TopKQueue, out chan<- []Pair) bool {
	pairs, _ := pairQueue.Descending()
	for i := range pairs {
		pair := pairs[i].(Pair)
		if !a.hasPartialTable(pair.CandTableID) {
			a.tableQueues[pair.CandTableID] = pqueue.NewTopKQueue(a.k)
			a.partialAlign[pair.CandTableID] = counter.NewCounter()
			a.reverseAlign[pair.CandTableID] = counter.NewCounter()
		}
		if a.hasSeenBetter(pair) {
			// because we are using priority queue
			continue
		}
		a.partialAlign[pair.CandTableID].Update(pair.CandColIndex)
		a.reverseAlign[pair.CandTableID].Update(pair.QueryColIndex)
		a.tableQueues[pair.CandTableID].Push(pair, pair.Sim)
		// When we get k unique column alignments for a candidate table
		if a.tableQueues[pair.CandTableID].Size() == a.k {
			out <- a.get(pair.CandTableID)
			a.completedTables.Update(pair.CandTableID)
		}
		// Check if we are done
		if a.completedTables.Unique() == a.n {
			return true
		}
	}
	return a.completedTables.Unique() == a.n
}

func (index *UnionIndex) QueryOrderAll(query [][]float64, N, K int) <-chan []Pair {
	log.Printf("Started querying the index with %d columns.", len(query))
	results := make(chan []Pair)
	go func() {
		defer close(results)
		alignment := initAlignment(K, N)
		batch := pqueue.NewTopKQueue(batchSize)
		done := make(chan struct{})
		defer close(done)
		for pair := range index.lsh.QueryPlus(query, done) {
			count++
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			// discard columns of already aligned tables
			if alignment.hasCompleted(tableID) {
				continue
			}
			e := getColumnPair(tableID, index.domainDir, columnIndex, pair.QueryIndex, query)
			batch.Push(e, e.Sim)
			if batch.Size() < batchSize {
				continue
			}
			// Process the batch
			if finished := alignment.processPairs(batch, results); finished {
				return
			}
		}
		// Don't forget remaining pairs in the queue
		if !batch.Empty() {
			alignment.processPairs(batch, results)
		}
	}()
	return results
}

func getColumnPair(candTableID, domainDir string, candColIndex, queryColIndex int, query [][]float64) Pair {
	// getting the embedding of the candidate column
	embFilename := getEmbFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(embFilename); os.IsNotExist(err) {
		log.Printf("Embedding file %s does not exist.", embFilename)
		panic(err)
	}
	vec, err := embedding.ReadVecFromDisk(embFilename, ByteOrder)
	if err != nil {
		log.Printf("Error in reading %s from disk.", embFilename)
		panic(err)
	}
	// inserting the pair into its corresponding priority queue
	cosine := embedding.Cosine(vec, query[queryColIndex])
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Sim:           cosine,
	}
	return p
}
