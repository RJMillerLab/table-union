package unionserver

import (
	"encoding/binary"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"

	minhashlsh "github.com/RJMillerLab/table-union/minhash-lsh"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/pqueue"
)

type JaccardUnionIndex struct {
	lsh       *minhashlsh.MinhashLSH
	domainDir string
	byteOrder binary.ByteOrder
	numHash   int
}

func NewJaccardUnionIndex(domainDir string, lsh *minhashlsh.MinhashLSH, numHash int) *JaccardUnionIndex {
	index := &JaccardUnionIndex{
		lsh:       lsh,
		domainDir: domainDir,
		byteOrder: ByteOrder,
		numHash:   numHash,
	}
	return index
}

func (index *JaccardUnionIndex) Build() error {
	domainfilenames := opendata.StreamFilenames()
	minhashFilenames := opendata.StreamMinhashVectors(10, domainfilenames)
	count := 0
	for file := range minhashFilenames {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			log.Printf("Simhash file does not exist.")
			continue
		}
		count += 1
		if count%1000 == 0 {
			log.Printf("indexed %d domains", count)
		}
		vec, err := opendata.ReadMinhashSignature(file, index.numHash)
		if err != nil {
			log.Printf("Error in reading minhash %s from disk.", file)
			return err
		}
		tableID, columnIndex := parseFilename(index.domainDir, file)
		index.lsh.Add(toColumnID(tableID, columnIndex), vec)
	}
	index.lsh.Index()
	return nil
}

func (index *JaccardUnionIndex) QueryOrderAll(query [][]uint64, N, K int) <-chan []Pair {
	log.Printf("Started querying the minhash index with %d columns.", len(query))
	results := make(chan []Pair)
	querySigs := make([]minhashlsh.Signature, len(query))
	go func() {
		defer close(results)
		// cast the type of query columns to Signature
		for i := 0; i < len(query); i++ {
			querySigs[i] = minhashlsh.Signature(query[i])
		}
		alignment := initAlignment(K, N)
		batch := pqueue.NewTopKQueue(batchSize)
		done := make(chan struct{})
		defer close(done)
		for pair := range index.lsh.QueryPlus(querySigs, done) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			// discard columns of already aligned tables
			if alignment.hasCompleted(tableID) {
				continue
			}
			e := getColumnPairJaccard(tableID, index.domainDir, columnIndex, pair.QueryIndex, index.numHash, query)
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

func getColumnPairJaccard(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query [][]uint64) Pair {
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("Embedding file %s does not exist.", minhashFilename)
		panic(err)
	}
	vec, err := opendata.ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", minhashFilename)
		panic(err)
	}
	// inserting the pair into its corresponding priority queue
	jaccard := estimatedJaccard(vec, query[queryColIndex])
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Sim:           jaccard,
	}
	return p
}
