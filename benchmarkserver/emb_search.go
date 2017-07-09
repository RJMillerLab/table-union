package benchmarkserver

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/ekzhu/counter"
	"github.com/gonum/matrix/mat64"
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

type SearchResult struct {
	CandidateTableID string
	Alignment        []Pair
	K                int
	N                int
	Duration         float64
}

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
			log.Printf("%s not found.", file)
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
	startTime       time.Time
}

func initAlignment(K, N int) alignment {
	return alignment{
		completedTables: counter.NewCounter(),
		partialAlign:    make(map[string](*counter.Counter)),
		reverseAlign:    make(map[string](*counter.Counter)),
		tableQueues:     make(map[string]*pqueue.TopKQueue),
		k:               K,
		n:               N,
		startTime:       time.Now(),
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
	return a.partialAlign[pair.CandTableID].Has(pair.CandColIndex) ||
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

func (a alignment) processPairs(pairQueue *pqueue.TopKQueue, out chan<- SearchResult) bool {
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
		a.tableQueues[pair.CandTableID].Push(pair, pair.T2)
		// When we get k unique column alignments for a candidate table
		if a.tableQueues[pair.CandTableID].Size() == a.k {
			a.completedTables.Update(pair.CandTableID)
			result := SearchResult{
				CandidateTableID: pair.CandTableID,
				Alignment:        a.get(pair.CandTableID),
				K:                a.k,
				N:                a.completedTables.Unique(),
				Duration:         float64(time.Now().Sub(a.startTime)) / float64(1000000),
			}
			out <- result
		}
		// Check if we are done
		if a.completedTables.Unique() == a.n {
			return true
		}
	}
	return a.completedTables.Unique() == a.n
}

func (index *UnionIndex) QueryOrderAll(query, queryCovar [][]float64, N, K int, queryCardinality []int) <-chan SearchResult {
	log.Printf("Started querying the index with %d columns.", len(query))
	results := make(chan SearchResult)
	go func() {
		defer close(results)
		alignment := initAlignment(K, N)
		batch := pqueue.NewTopKQueue(batchSize)
		done := make(chan struct{})
		defer close(done)
		for pair := range index.lsh.QueryPlus(query, done) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			// discard columns of already aligned tables
			if alignment.hasCompleted(tableID) {
				continue
			}
			//e := getColumnPairPlus(tableID, index.domainDir, columnIndex, pair.QueryIndex, query[pair.QueryIndex], queryCovar[pair.QueryIndex], queryCardinality[pair.QueryIndex])
			e := getColumnPair(tableID, index.domainDir, columnIndex, pair.QueryIndex, query[pair.QueryIndex])
			//batch.Push(e, e.T2)
			batch.Push(e, e.Cosine)
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

func getColumnPair(candTableID, domainDir string, candColIndex, queryColIndex int, query []float64) Pair {
	// getting the embedding of the candidate column
	//embFilename := getEmbFilename(candTableID, domainDir, candColIndex)
	embFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-sum", candTableID, candColIndex))
	//embFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-mean", candTableID, candColIndex))
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
	cosine := embedding.Cosine(vec, query)
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Cosine:        cosine,
		Sim:           cosine,
	}
	return p
}

func getColumnPairPlus(candTableID, domainDir string, candColIndex, queryColIndex int, queryMean, queryCovar []float64, queryCardinality int) Pair {
	// getting the embedding of the candidate column
	meanFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-mean", candTableID, candColIndex))
	if _, err := os.Stat(meanFilename); os.IsNotExist(err) {
		log.Printf("Mean embedding file %s does not exist.", meanFilename)
		panic(err)
	}
	mean, err := embedding.ReadVecFromDisk(meanFilename, ByteOrder)
	if err != nil {
		log.Printf("Error in reading %s from disk.", meanFilename)
		panic(err)
	}
	// reading covariance matrix
	covarFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-covar", candTableID, candColIndex))
	if _, err := os.Stat(covarFilename); os.IsNotExist(err) {
		log.Printf("Embedding file %s does not exist.", covarFilename)
		panic(err)
	}
	covar, err := embedding.ReadVecFromDisk(covarFilename, ByteOrder)
	if err != nil {
		log.Printf("Error in reading %s from disk.", covarFilename)
		panic(err)
	}
	card := getDomainCardinality(candTableID, domainDir, candColIndex)
	cosine := embedding.Cosine(mean, queryMean)
	// inserting the pair into its corresponding priority queue
	ht2, f := getT2Statistics(mean, queryMean, covar, queryCovar, card, queryCardinality)
	if math.IsNaN(ht2) || math.IsNaN(f) || math.IsInf(ht2, 0) || math.IsInf(f, 0) {
		ht2 = 1000
		f = 1000
	}
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Cosine:        cosine,
		T2:            ht2,
		F:             f,
		Sim:           ht2,
	}
	return p
}

func getT2Statistics(m1, m2 []float64, cv1, cv2 []float64, card1, card2 int) (float64, float64) {
	//dim := int(math.Sqrt(float64(len(cv1))))
	dim := len(cv1)
	cvd1 := mat64.NewDense(dim, dim, getCovarMatrix(cv1))
	cvd2 := mat64.NewDense(dim, dim, getCovarMatrix(cv2))
	t1 := mat64.NewDense(0, 0, nil)
	t2 := mat64.NewDense(0, 0, nil)
	t1.Scale(float64(card1-1), cvd1)
	t2.Scale(float64(card2-1), cvd2)
	t3 := mat64.NewDense(0, 0, nil)
	t3.Add(t1, t2)
	t4 := mat64.NewDense(0, 0, nil)
	t4.Scale(1.0/float64(card1+card2-2), t3)
	pooledCovar := mat64.NewDense(0, 0, nil)
	pooledCovar.Scale((1.0/float64(card1) + 1.0/float64(card2)), t4)
	pcInverse := mat64.NewDense(0, 0, nil)
	pcInverse.Inverse(pooledCovar)
	md1 := mat64.NewDense(1, dim, m1)
	md2 := mat64.NewDense(1, dim, m2)
	meanDiff := mat64.NewDense(0, 0, nil)
	meanDiff.Sub(md1, md2)
	p1 := mat64.NewDense(0, 0, nil)
	p1.Mul(meanDiff, pcInverse)
	ht2 := mat64.NewDense(0, 0, nil)
	ht2.Mul(p1, meanDiff.T())
	T2 := ht2.At(0, 0)
	// Computing F-distribution
	p := len(m1)
	n := card1 + card2 - 1
	f := (float64(n-p) / math.Abs(float64(p*(n-1)))) * T2
	return T2, f
}

func getCovarMatrix(variance []float64) []float64 {
	dim := len(variance)
	covariance := make([]float64, dim*dim)
	for i := 0; i < len(variance); i++ {
		for j := 0; j < len(variance); j++ {
			if i == j {
				covariance[i*dim+j] = variance[i]
				continue
			}
			covariance[i*dim+j] = 0.0
		}
	}
	return covariance
}
