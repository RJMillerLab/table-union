package benchmarkserver

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/pqueue"
	"github.com/RJMillerLab/table-union/simhashlsh"
)

type UnionPercIndex struct {
	lsh       *simhashlsh.CosineLSH
	domainDir string
	byteOrder binary.ByteOrder
	tableCDF  opendata.CDF
	attCDF    opendata.CDF
}

func NewUnionPercIndex(domainDir string, lsh *simhashlsh.CosineLSH) *UnionPercIndex {
	attCDF, tableCDF := opendata.LoadCDF()
	index := &UnionPercIndex{
		lsh:       lsh,
		domainDir: domainDir,
		byteOrder: ByteOrder,
		tableCDF:  tableCDF,
		attCDF:    attCDF,
	}
	return index
}

func (index *UnionPercIndex) Build() error {
	start := getNow()
	domainfilenames := opendata.StreamFilenames()
	embfilenames := opendata.StreamEmbVectors(10, domainfilenames)
	//embfilenames := StreamAllODEmbVectors(10, domainfilenames)
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
	log.Printf("index time for embedding: %f", getNow()-start)
	return nil
}

func (index *UnionPercIndex) QueryOrderAll(query, queryCovar [][]float64, N, K int, queryCardinality []int) <-chan SearchResult {
	//start := getNow()
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
			e := getColumnPairPercentile(tableID, index.domainDir, columnIndex, pair.QueryIndex, query[pair.QueryIndex], queryCovar[pair.QueryIndex], queryCardinality[pair.QueryIndex], index.attCDF)
			if !math.IsNaN(e.T2) && !math.IsNaN(e.F) && !math.IsInf(e.T2, 0) && !math.IsInf(e.F, 0) {
				if e.Cosine != 0.0 {
					//batch.Push(e, e.Cosine)
					batch.Push(e, e.Percentile)
					//batch.Push(e, -1.0*e.T2)
					//if e.Cosine > 0.9 && e.T2 > 100 {
					//	log.Printf("anomaly: c: %f t2: %f", e.Cosine, e.T2)
					//}
				}
			}
			if batch.Size() < batchSize {
				continue
			}
			// Process the batch
			if finished := alignment.processPairsEmbedding(batch, results); finished {
				//log.Printf("elapse time: %f", getNow()-start)
				return
			}
		}
		// Don't forget remaining pairs in the queue
		if !batch.Empty() {
			alignment.processPairsEmbedding(batch, results)
		}
	}()
	return results
}

func getColumnPairPercentile(candTableID, domainDir string, candColIndex, queryColIndex int, queryMean, queryCovar []float64, queryCardinality int, attCDF opendata.CDF) Pair {
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
	//covarFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-covar", candTableID, candColIndex))
	//if _, err := os.Stat(covarFilename); os.IsNotExist(err) {
	//	log.Printf("Embedding file %s does not exist.", covarFilename)
	//	panic(err)
	//}
	//covar, err := embedding.ReadVecFromDisk(covarFilename, ByteOrder)
	//if err != nil {
	//	log.Printf("Error in reading %s from disk.", covarFilename)
	//	panic(err)
	//}
	card := getDomainSize(candTableID, domainDir, candColIndex)
	cosine := embedding.Cosine(mean, queryMean)
	// inserting the pair into its corresponding priority queue
	//ht2, f := getT2Statistics(mean, queryMean, covar, queryCovar, card, queryCardinality)
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Cosine:        cosine,
		//T2:               math.Abs(ht2),
		//F:                math.Abs(f),
		//Sim:              math.Abs(ht2),
		Sim:              cosine,
		QueryCardinality: queryCardinality,
		CandCardinality:  card,
		Percentile:       getPercentile(attCDF, cosine),
	}
	return p
}
