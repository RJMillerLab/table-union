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
	//minhashFilenames := opendata.StreamMinhashVectors(10, "ont-minhash-l2", domainfilenames)
	count := 0
	for file := range minhashFilenames {
		log.Printf("insert: %s", file)
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
	minhashFilenames := opendata.StreamMinhashVectors(10, "noann-minhash", domainfilenames)
	count := 0
	for file := range minhashFilenames {
		log.Printf("no ont insert: %s", file)
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

func (server *OntologyJaccardServer) OntQueryOrderAll(noOntQuery, ontQuery, query [][]uint64, N, K int, noOntQueryCard, ontQueryCard, queryCard []int) <-chan SearchResult {
	log.Printf("Started querying the minhash index with %d columns.", len(query))
	results := make(chan SearchResult)
	querySigs := make([]minhashlsh.Signature, len(query))
	ontQuerySigs := make([]minhashlsh.Signature, len(ontQuery))
	noOntQuerySigs := make([]minhashlsh.Signature, len(noOntQuery))
	// cast the type of query columns to Signature
	for i := 0; i < len(query); i++ {
		querySigs[i] = minhashlsh.Signature(query[i])
	}
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
		for pair := range server.ui.lsh.QueryPlus(querySigs, done1) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.ui.domainDir, columnIndex, pair.QueryIndex, server.ui.numHash, query[pair.QueryIndex], ontQuery[pair.QueryIndex], noOntQuery[pair.QueryIndex], queryCard[pair.QueryIndex], ontQueryCard[pair.QueryIndex], noOntQueryCard[pair.QueryIndex])
			if e.Sim != 0.0 {
				reduceBatch <- e
			}
		}
		wg.Done()
	}()
	go func() {
		for pair := range server.oi.lsh.QueryPlus(ontQuerySigs, done2) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.ui.domainDir, columnIndex, pair.QueryIndex, server.ui.numHash, query[pair.QueryIndex], ontQuery[pair.QueryIndex], noOntQuery[pair.QueryIndex], queryCard[pair.QueryIndex], ontQueryCard[pair.QueryIndex], noOntQueryCard[pair.QueryIndex])
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
				if finished := alignment.processPairsPlus(reduceQueue, results); finished {
					close(done1)
					close(done2)
				}
				reduceQueue = pqueue.NewTopKQueue(batchSize)
			}
		}
		wwg.Done()
	}()
	go func() {
		wwg.Wait()
		log.Printf("done with the index go routines")
		close(results)
	}()

	go func() {
		wg.Wait()
		log.Printf("done with processing the reduce batch")
		close(reduceBatch)
	}()

	return results
}

func (a alignment) processPairsPlus(reduceQueue *pqueue.TopKQueue, out chan<- SearchResult) bool {
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

func getColumnPairOntJaccard(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query [][]uint64, ontQuery [][]uint64) Pair {
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
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
	jaccard := estimateJaccard(vec, query[queryColIndex])
	// computing ontology jaccard
	ontMinhashFilename := getOntMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		//log.Printf("Minhash file %s does not exist.", ontMinhashFilename)
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

func getColumnPairOntJaccardPlusPlus(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query []uint64, ontQuery []uint64, noOntQuery []uint64, queryCard, ontQueryCard, noOntQueryCard int) Pair {
	// getting the embedding of the candidate column
	//minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
	minhashFilename := getUnannotatedMinhashFilename(candTableID, domainDir, candColIndex)
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
	jaccard := estimateJaccard(vec, query)
	// computing ontology jaccard
	ontMinhashFilename := getOntMinhashFilename(candTableID, domainDir, candColIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		//  log.Printf("Minhash file %s does not exist.", ontMinhashFilename)
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
	ontJaccard := estimateJaccard(vec, ontQuery)
	noA, nA := getOntDomainCardinality(candTableID, domainDir, candColIndex)
	nB := queryCard
	noB := ontQueryCard
	jaccardProb := sameDomainProb(jaccard, nA, nB)
	ontProb := sameDomainProb(ontJaccard, noA, noB)
	p := Pair{
		QueryColIndex:          queryColIndex,
		CandTableID:            candTableID,
		CandColIndex:           candColIndex,
		Jaccard:                jaccard,
		Hypergeometric:         jaccardProb,
		OntologyJaccard:        ontJaccard,
		OntologyHypergeometric: ontProb,
		Sim: jaccardProb + ontProb - (jaccardProb * ontProb),
	}
	return p
}

func getColumnPairOntJaccardPlus(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query []uint64, ontQuery []uint64, noOntQuery []uint64, queryCard, ontQueryCard, noOntQueryCard int) Pair {
	delta := 0.000001
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
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
	jaccard := estimateJaccard(vec, query)
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
		//panic(err)
		return Pair{
			QueryColIndex: queryColIndex,
			CandTableID:   candTableID,
			CandColIndex:  candColIndex,
			Sim:           0.0,
		}
	}
	ontJaccard := estimateJaccard(vec, ontQuery)
	noA, nA := getOntDomainCardinality(candTableID, domainDir, candColIndex)
	nB := queryCard
	noB := ontQueryCard
	coverage := float64(queryCard-noOntQueryCard) / float64(queryCard)
	jaccardProb := sameDomainProb(jaccard, nA, nB) + delta
	ontProb := sameDomainProb(ontJaccard, noA, noB) + delta
	log.Printf("ontProb: %f", ontProb)
	log.Printf("Coverage ontProb: %f", (1-coverage)*jaccardProb+coverage*ontProb)
	p := Pair{
		QueryColIndex:          queryColIndex,
		CandTableID:            candTableID,
		CandColIndex:           candColIndex,
		Jaccard:                jaccard,
		Hypergeometric:         jaccardProb,
		OntologyJaccard:        ontJaccard,
		OntologyHypergeometric: ontProb,
		Sim: (1-coverage)*jaccardProb + coverage*ontProb,
		//Sim: ontProb,
	}
	return p
}

func getColumnPairOntology(candTableID, domainDir string, candColIndex, queryColIndex, numHash int, query [][]uint64, ontQuery [][]uint64, queryCard, ontQueryCard int) Pair {
	delta := 0.000001
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candTableID, domainDir, candColIndex)
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
	jaccard := estimateJaccard(vec, query[queryColIndex])
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
		//panic(err)
		return Pair{
			QueryColIndex: queryColIndex,
			CandTableID:   candTableID,
			CandColIndex:  candColIndex,
			Sim:           0.0,
		}
	}
	ontJaccard := estimateJaccard(vec, ontQuery[queryColIndex])
	noA, nA := getOntDomainCardinality(candTableID, domainDir, candColIndex)
	nB := queryCard
	noB := ontQueryCard
	jaccardProb := sameDomainProb(jaccard, nA, nB) + delta
	ontProb := sameDomainProb(ontJaccard, noA, noB) + delta
	p := Pair{
		QueryColIndex:          queryColIndex,
		CandTableID:            candTableID,
		CandColIndex:           candColIndex,
		Jaccard:                jaccard,
		Hypergeometric:         jaccardProb,
		OntologyJaccard:        ontJaccard,
		OntologyHypergeometric: ontProb,
		Sim: jaccardProb + ontProb - (jaccardProb * ontProb),
	}
	return p
}
