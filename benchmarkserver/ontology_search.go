package benchmarkserver

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/ekzhu/counter"
	_ "github.com/mattn/go-sqlite3"

	minhashlsh "github.com/RJMillerLab/table-union/minhashlsh"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/pqueue"
)

func (index *JaccardUnionIndex) OntBuild() error {
	log.Printf("ont build")
	domainfilenames := opendata.StreamFilenames()
	minhashFilenames := opendata.StreamMinhashVectors(10, "ont-minhash-l1", domainfilenames)
	count := 0
	for file := range minhashFilenames {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			log.Printf("Minhash file does not exist: %s", file)
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
	log.Printf("ont build count %d", count)
	return nil
}

func (index *JaccardUnionIndex) NoOntBuild() error {
	log.Printf("no ont build")
	domainfilenames := opendata.StreamFilenames()
	minhashFilenames := opendata.StreamMinhashVectors(10, "noann-minhash", domainfilenames)
	count := 0
	for file := range minhashFilenames {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			log.Printf("Minhash file does not exist: %s", file)
			continue
		}
		count += 1
		if count%10 == 0 {
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
	log.Printf("no ont build count %d", count)
	return nil
}

func (server *OntologyJaccardServer) OntQueryOrderAll(query, ontQuery [][]uint64, N, K int) <-chan SearchResult {
	log.Printf("Started querying the minhash index with %d columns.", len(query))
	results := make(chan SearchResult)
	querySigs := make([]minhashlsh.Signature, len(query))
	ontQuerySigs := make([]minhashlsh.Signature, len(ontQuery))
	// cast the type of query columns to Signature
	for i := 0; i < len(query); i++ {
		querySigs[i] = minhashlsh.Signature(query[i])
	}
	for i := 0; i < len(ontQuery); i++ {
		ontQuerySigs[i] = minhashlsh.Signature(ontQuery[i])
	}
	alignment := initAlignment(K, N)
	batchPairs := make(map[Pair]bool)
	batch := pqueue.NewTopKQueue(batchSize)
	oBatch := pqueue.NewTopKQueue(batchSize)
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		done := make(chan struct{})
		defer close(done)
		for pair := range server.ui.lsh.QueryPlus(querySigs, done) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			// discard columns of already aligned tables
			if alignment.hasCompleted(tableID) {
				continue
			}
			e := getColumnPairOntJaccard(tableID, server.ui.domainDir, columnIndex, pair.QueryIndex, server.ui.numHash, query, ontQuery)
			if _, ok := batchPairs[e]; !ok {
				if e.Sim != 0.0 {
					log.Printf("sim is %f", e.Sim)
					batchPairs[e] = true
					batch.Push(e, e.Sim)
				}
			}
			log.Printf("1: batch size: %d, ont batch size: %d >= %d", batch.Size(), oBatch.Size(), batchSize)
			if (batch.Size() + oBatch.Size()) < batchSize {
				continue
			}
			batchPairs = make(map[Pair]bool)
			// Process the batch
			if finished := alignment.processPairsPlus(batch, oBatch, results); finished {
				return
			}
		}
		// Don't forget remaining pairs in the queue
		if !batch.Empty() || !oBatch.Empty() {
			alignment.processPairsPlus(batch, oBatch, results)
		}
		wg.Done()
	}()
	go func() {
		done := make(chan struct{})
		defer close(done)
		for pair := range server.oi.lsh.QueryPlus(ontQuerySigs, done) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			log.Printf("1.cand: %s", pair.CandidateKey)
			// discard columns of already aligned tables
			if alignment.hasCompleted(tableID) {
				continue
			}
			e := getColumnPairOntJaccard(tableID, server.oi.domainDir, columnIndex, pair.QueryIndex, server.oi.numHash, query, ontQuery)
			if _, ok := batchPairs[e]; !ok {
				if e.Sim != 0.0 {
					log.Printf("sim2 is %f", e.Sim)
					batchPairs[e] = true
					oBatch.Push(e, e.Sim)
				}
			}
			if (batch.Size() + oBatch.Size()) < batchSize {
				continue
			}
			batchPairs = make(map[Pair]bool)
			// Process the batch
			if finished := alignment.processPairsPlus(batch, oBatch, results); finished {
				return
			}
		}
		// Don't forget remaining pairs in the queue
		if !batch.Empty() || !oBatch.Empty() {
			alignment.processPairsPlus(batch, oBatch, results)
		}
		wg.Done()
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

func (a alignment) processPairsPlus(pairQueue, ontPairQueue *pqueue.TopKQueue, out chan<- SearchResult) bool {
	reduceQueue := pqueue.NewTopKQueue(pairQueue.Size() + ontPairQueue.Size())
	if pairQueue.Size() != 0 {
		for pairQueue.Empty() {
			p, s := pairQueue.Pop()
			reduceQueue.Push(p, s)
		}
	}
	if ontPairQueue.Size() != 0 {
		for ontPairQueue.Empty() {
			p, s := ontPairQueue.Pop()
			reduceQueue.Push(p, s)
		}
	}
	pairs, _ := reduceQueue.Descending()
	for i := range pairs {
		pair := pairs[i].(Pair)
		log.Printf("pair: %s.%d", pair.CandTableID, pair.CandColIndex)
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

func getColumnPairOntJaccard(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query [][]uint64, ontQuery [][]uint64) Pair {
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("Minhash file %s does not exist.", minhashFilename)
		//panic(err)
		return Pair{
			QueryColIndex: queryColIndex,
			CandTableID:   candTableID,
			CandColIndex:  candColIndex,
			Sim:           0.0,
		}
	}
	vec, err := opendata.ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", minhashFilename)
		//panic(err)
		return Pair{
			QueryColIndex: queryColIndex,
			CandTableID:   candTableID,
			CandColIndex:  candColIndex,
			Sim:           0.0,
		}
	}
	jaccard := estimateJaccard(vec, query[queryColIndex])
	// computing ontology jaccard
	ontMinhashFilename := getOntMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		log.Printf("Minhash file %s does not exist.", ontMinhashFilename)
		//panic(err)
		return Pair{
			QueryColIndex: queryColIndex,
			CandTableID:   candTableID,
			CandColIndex:  candColIndex,
			Sim:           0.0,
		}
	}
	vec, err = opendata.ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", ontMinhashFilename)
		//panic(err)
		return Pair{
			QueryColIndex: queryColIndex,
			CandTableID:   candTableID,
			CandColIndex:  candColIndex,
			Sim:           0.0,
		}
	}
	ontJaccard := estimateJaccard(vec, ontQuery[queryColIndex])
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Sim:           jaccard * ontJaccard,
	}
	return p
}

func getColumnPairOntJaccardPlus(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query [][]uint64, ontQuery [][]uint64, queryCard, ontQueryCard int) Pair {
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
	jaccard := estimateJaccard(vec, query[queryColIndex])
	// computing ontology jaccard
	ontMinhashFilename := getOntMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		log.Printf("Minhash file %s does not exist.", ontMinhashFilename)
		panic(err)
	}
	vec, err = opendata.ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", ontMinhashFilename)
		panic(err)
	}
	ontJaccard := estimateJaccard(vec, ontQuery[queryColIndex])
	noA, nA := getOntDomainCardinality(candTableID, domainDir, candColIndex)
	nB := queryCard
	noB := ontQueryCard
	p := Pair{
		QueryColIndex: queryColIndex,
		CandTableID:   candTableID,
		CandColIndex:  candColIndex,
		Sim:           sameDomainProb(jaccard, nA, nB) * sameDomainProb(ontJaccard, noA, noB),
	}
	log.Printf("sim: %f", p.Sim)
	return p
}
