package benchmarkserver

import (
	"log"
	"os"
	"strconv"
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
			//		log.Printf("Minhash file does not exist: %s", file)
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
	//minhashFilenames := opendata.StreamMinhashVectors(10, "minhash", domainfilenames)
	minhashFilenames := opendata.StreamMinhashVectors(10, "noann-minhash", domainfilenames)
	count := 0
	for file := range minhashFilenames {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			//log.Printf("Minhash file does not exist: %s", file)
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
	log.Printf("no ont build count %d", count)
	return nil
}

func (server *OntologyJaccardServer) OntQueryOrderAll(noOntQuery, ontQuery [][]uint64, N, K int, noOntQueryCard, ontQueryCard []int) <-chan SearchResult {
	results := make(chan SearchResult)
	//querySigs := make([]minhashlsh.Signature, len(query))
	ontQuerySigs := make([]minhashlsh.Signature, len(ontQuery))
	noOntQuerySigs := make([]minhashlsh.Signature, len(noOntQuery))
	// cast the type of query columns to Signature
	//for i := 0; i < len(query); i++ {
	//	querySigs[i] = minhashlsh.Signature(query[i])
	//}
	for i := 0; i < len(ontQuery); i++ {
		ontQuerySigs[i] = minhashlsh.Signature(ontQuery[i])
	}
	for i := 0; i < len(noOntQuery); i++ {
		noOntQuerySigs[i] = minhashlsh.Signature(noOntQuery[i])
	}
	alignment := initAlignment(K, N)
	reduceQueue := pqueue.NewTopKQueue(batchSize)
	reduceBatch := make(chan Pair)
	done1 := make(chan struct{})
	done2 := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for pair := range server.ui.lsh.QueryPlus(noOntQuerySigs, done1) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.ui.domainDir, columnIndex, pair.QueryIndex, server.ui.numHash, ontQuery[pair.QueryIndex], noOntQuery[pair.QueryIndex], ontQueryCard[pair.QueryIndex], noOntQueryCard[pair.QueryIndex])
			if e.Sim != 0.0 {
				reduceBatch <- e
			}
		}
		wg.Done()
	}()
	go func() {
		for pair := range server.oi.lsh.QueryPlus(ontQuerySigs, done2) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.ui.domainDir, columnIndex, pair.QueryIndex, server.ui.numHash, ontQuery[pair.QueryIndex], noOntQuery[pair.QueryIndex], ontQueryCard[pair.QueryIndex], noOntQueryCard[pair.QueryIndex])
			if e.Sim != 0.0 {
				reduceBatch <- e
			}
		}
		wg.Done()
	}()
	wwg := &sync.WaitGroup{}
	wwg.Add(1)
	go func() {
		for pair := range reduceBatch {
			if alignment.hasCompleted(pair.CandTableID) {
				continue
			}
			reduceQueue.Push(pair, pair.Sim)
			if reduceQueue.Size() == batchSize {
				if finished := alignment.processPairsOntology(reduceQueue, results); finished {
					close(done1)
					close(done2)
					wwg.Done()
					return
				}
				reduceQueue = pqueue.NewTopKQueue(batchSize)
			}
		}
		if reduceQueue.Size() != 0 {
			alignment.processPairsOntology(reduceQueue, results)
		}
		wwg.Done()
	}()
	go func() {
		wwg.Wait()
		close(results)
	}()

	go func() {
		wg.Wait()
		close(reduceBatch)
	}()

	return results
}

func (a alignment) processPairsOntology(reduceQueue *pqueue.TopKQueue, out chan<- SearchResult) bool {
	pairs, _ := reduceQueue.Descending()
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

func getColumnPairOntJaccardPlus(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, ontQuery, noOntQuery []uint64, ontQueryCard, noOntQueryCard int) Pair {
	// getting the embedding of the candidate column
	minhashFilename := getUnannotatedMinhashFilename(candTableID, domainDir, candColIndex)
	//minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		//log.Printf("Minhash file %s does not exist.", minhashFilename)
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
	jaccard := estimateJaccard(vec, noOntQuery)
	// computing ontology jaccard
	ontMinhashFilename := getOntMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		//	log.Printf("Minhash file %s does not exist.", ontMinhashFilename)
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
		return Pair{
			QueryColIndex: queryColIndex,
			CandTableID:   candTableID,
			CandColIndex:  candColIndex,
			Sim:           0.0,
		}
	}
	ontJaccard := estimateJaccard(vec, ontQuery)
	noA, nA := getOntDomainCardinality(candTableID, domainDir, candColIndex)
	nB := ontQueryCard
	noB := noOntQueryCard
	//	coverage := float64(queryCard-noOntQueryCard) / float64(queryCard)
	jaccardProb := sameDomainProb(jaccard, noA, noB)
	ontProb := sameDomainProb(ontJaccard, nA, nB)
	//log.Printf("tableID: %s.%d, annB: %d, noannB: %d, annA: %d, noAnnA: %d, j: %f, h: %f, oj: %f, oh: %f coverage sim: %f maxsim: %f", candTableID, candColIndex, nB, noB, nA, noA, jaccard, jaccardProb, ontJaccard, ontProb, (1-coverage)*jaccardProb+coverage*ontProb, math.Max(jaccardProb, ontProb))
	p := Pair{
		QueryColIndex:          queryColIndex,
		CandTableID:            candTableID,
		CandColIndex:           candColIndex,
		Jaccard:                jaccard,
		Hypergeometric:         jaccardProb,
		OntologyJaccard:        ontJaccard,
		OntologyHypergeometric: ontProb,
		//Sim: (1-coverage)*jaccardProb + coverage*ontProb,
		//Sim: ontProb,
		//Sim: jaccardProb,
		Sim: ontProb + jaccardProb - ontProb*jaccardProb,
		//Sim: ontProb * jaccardProb,
		//Sim: math.Max(jaccardProb, ontProb),
	}
	//log.Printf("ontProb: %f, jaccardProb: %f, sim: %f", ontProb, jaccardProb, p.Sim)
	return p
}
