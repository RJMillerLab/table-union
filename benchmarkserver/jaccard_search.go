package benchmarkserver

import (
	"encoding/binary"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/ekzhu/counter"
	_ "github.com/mattn/go-sqlite3"

	minhashlsh "github.com/RJMillerLab/table-union/minhashlsh"
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
	start := getNow()
	domainfilenames := opendata.StreamFilenames()
	minhashFilenames := opendata.StreamMinhashVectors(10, "minhash", domainfilenames)
	count := 0
	for file := range minhashFilenames {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			log.Printf("Simhash file does not exist: %s", file)
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
	log.Printf("index time for jaccard: %f", getNow()-start)
	return nil
}

func (index *JaccardUnionIndex) QueryOrderAll(query [][]uint64, N, K int, queryCardinality []int) <-chan SearchResult {
	log.Printf("Started querying the minhash index with %d columns.", len(query))
	results := make(chan SearchResult)
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
			//e := getColumnPairJaccard(tableID, index.domainDir, columnIndex, pair.QueryIndex, index.numHash, query)
			e := getColumnPairJaccardPlus(tableID, index.domainDir, columnIndex, pair.QueryIndex, index.numHash, query, queryCardinality[pair.QueryIndex])
			if e.Sim != 0.0 {
				batch.Push(e, e.Sim)
			}
			if batch.Size() < batchSize {
				continue
			}
			// Process the batch
			if finished := alignment.processPairsSyntactic(batch, results); finished {
				return
			}
		}
		// Don't forget remaining pairs in the queue
		if !batch.Empty() {
			alignment.processPairsSyntactic(batch, results)
		}
	}()
	return results
}

func (a alignment) processPairsSyntactic(pairQueue *pqueue.TopKQueue, out chan<- SearchResult) bool {
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
		a.partialAlign[pair.CandTableID].Update(strconv.Itoa(pair.CandColIndex))
		a.reverseAlign[pair.CandTableID].Update(strconv.Itoa(pair.QueryColIndex))
		a.tableQueues[pair.CandTableID].Push(pair, pair.Hypergeometric)
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

func getColumnPairJaccardPlus(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query [][]uint64, queryCardinality int) Pair {
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
	//if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
	//	log.Printf("Minhash file %s does not exist.", minhashFilename)
	//	panic(err)
	//}
	vec, err := opendata.ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", minhashFilename)
		panic(err)
	}
	// inserting the pair into its corresponding priority queue
	jaccard := estimateJaccard(vec, query[queryColIndex])
	nB := getDomainCardinality(candTableID, domainDir, candColIndex)
	nA := queryCardinality
	//containment := (jaccard * (float64(nA + nB))) / ((1.0 + jaccard) * float64(nA))
	sig := sameDomainProb(jaccard, nA, nB)
	if sig >= 0.99 {
		log.Printf("nA: %d, nB: %d, jaccard: %f", nA, nB, jaccard)
	}
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Jaccard:       jaccard,
		//	Containment:    containment,
		Hypergeometric: sig,
		Sim:            sig,
		//Sim: jaccard,
	}
	return p
}

func getColumnPairJaccard(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query [][]uint64) Pair {
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("Minhash file %s does not exist.", minhashFilename)
		panic(err)
	}
	vec, err := opendata.ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", minhashFilename)
		panic(err)
	}
	// inserting the pair into its corresponding priority queue
	jaccard := estimateJaccard(vec, query[queryColIndex])
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Jaccard:       jaccard,
		Sim:           jaccard,
	}
	return p
}

func sameDomainProbPlus(estimatedJaccard float64, nA, nB int) float64 {
	N := nA + nB
	k := int(math.Floor((estimatedJaccard * float64(N)) / (1.0 + estimatedJaccard)))
	if (k == nA || k == nB) && (nA != nB) {
		return 0.0
	}
	if k > nA {
		log.Printf("invalid intersection")
	}
	F_k_A_B := 0.0
	for i := 0; i < k; i++ {
		F_k_A_B += hyperGeometricProb(i, nA, nB, N)
	}
	if F_k_A_B > 1.0 {
		log.Printf("jaccard: %f, intersection: %d, querySize: %d, candSize: %d, D: %d, significance: %f", estimatedJaccard, k, nA, nB, N, F_k_A_B)
	}
	return F_k_A_B
}

func sameDomainProb(estimatedJaccard float64, nA, nB int) float64 {
	N := nA + nB
	k := int(math.Floor((estimatedJaccard * float64(N)) / (1.0 + estimatedJaccard)))
	if k > nA || k > nB {
		k = int(math.Min(float64(nA), float64(nB)))
	}
	F_k_A_B := 0.0
	for i := 0; i <= k; i++ {
		F_k_A_B += math.Exp(logHyperGeometricProb(i, nA, nB, N))
	}
	if F_k_A_B > 2.0 {
		log.Printf("jaccard: %f, intersection: %d, querySize: %d, candSize: %d, D: %d, significance: %f", estimatedJaccard, k, nA, nB, N, F_k_A_B)
	}
	return F_k_A_B
}

func hyperGeometricProb(k, K, n, N int) float64 {
	hgp := float64(combination(K, k)*combination(N-K, n-k)) / float64(combination(N, n))
	return hgp
}

func logHyperGeometricProb(k, K, n, N int) float64 {
	hgp := logCombination(K, k) + logCombination(N-K, n-k) - logCombination(N, n)
	return hgp
}

func combination(np, kp int) int64 {
	if kp == 0 {
		return 1
	}
	var r int64
	k := int64(kp)
	n := int64(np)
	r = 1
	if k > n {
		return 0
	}
	for i := int64(1); i <= k; i++ {
		r = r * n
		n = n - 1
		r = r / i
	}
	return r
}

func logCombination(m, n int) float64 {
	a := 0.0
	b := 0.0
	//for i := n + 1; i < (m + 1); i++ {
	for i := n + 1; i < m+1; i++ {
		a += math.Log(float64(i))
	}
	for i := 1; i < (m - n + 1); i++ {
		b += math.Log(float64(i))
	}
	return a - b

}
