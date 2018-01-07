package benchmarkserver

import (
	"log"
	"sync"
	"time"

	"github.com/ekzhu/counter"
	"github.com/fnargesian/pqueuespan"
	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/minhashlsh"
	"github.com/RJMillerLab/table-union/opendata"
)

var (
	delta = 0.1
)

type tablePair struct {
	queryTable     string
	candidateTable string
}

func initCAlignment(N int, tableCDF map[int]opendata.CDF, attCDFs map[string]opendata.CDF, domainDir string, perturbationDelta float64) alignment {
	return alignment{
		completedTables:   counter.NewCounter(),
		partialAlign:      make(map[string](*counter.Counter)),
		reverseAlign:      make(map[string](*counter.Counter)),
		tableSpanQueues:   make(map[string]*pqueuespan.TopKQueue),
		n:                 N,
		startTime:         time.Now(),
		tableCDF:          tableCDF,
		attCDFs:           attCDFs,
		domainDir:         domainDir,
		perturbationDelta: perturbationDelta,
	}
}

func (server *CombinedServer) CombinedOrderAll(nlMeans, nlCovars [][]float64, setVecs, noOntVecs, ontVecs [][]uint64, N int, noOntCards, ontCards, nlCards, setCards []int, queryTableID string) <-chan SearchResult {
	var numBatches int
	results := make(chan SearchResult)
	log.Printf("search queryTableID: %s", queryTableID)
	ontSigs := make([]minhashlsh.Signature, len(ontVecs))
	noOntSigs := make([]minhashlsh.Signature, len(noOntVecs))
	setSigs := make([]minhashlsh.Signature, len(setVecs))
	// cast the type of query columns to Signature
	for i := 0; i < len(ontVecs); i++ {
		ontSigs[i] = minhashlsh.Signature(ontVecs[i])
	}
	for i := 0; i < len(noOntVecs); i++ {
		noOntSigs[i] = minhashlsh.Signature(noOntVecs[i])
	}
	for i := 0; i < len(setVecs); i++ {
		setSigs[i] = minhashlsh.Signature(setVecs[i])
	}
	alignment := initCAlignment(N, server.tableCDF, server.attCDFs, server.seti.domainDir, server.perturbationDelta)
	//reduceQueue := pqueue.NewTopKQueue(batchSize)
	reduceQueue := pqueuespan.NewTopKQueue(batchSize)
	reduceBatch := make(chan Pair)
	done1 := make(chan struct{})
	done2 := make(chan struct{})
	done3 := make(chan struct{})
	done4 := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(4)
	go func() {
		defer wg.Done()
		if len(nlMeans) == 0 {
			return
		}
		for pair := range server.nli.lsh.QueryPlus(nlMeans, done3) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairPlus(tableID, server.seti.domainDir, columnIndex, pair.QueryIndex, nlMeans[pair.QueryIndex], nlCovars[pair.QueryIndex], nlCards[pair.QueryIndex])
			//e.Percentile = getPercentile(server.attCDFs["nl"], e.Sim)
			e.Percentile = opendata.GetPerturbedPercentile(server.attCDFs["nl"], e.Sim, server.perturbationDelta)
			//e.Measure = "nl"
			if e.Percentile.Value != 0.0 {
				//reduceBatch <- e
				select {
				case reduceBatch <- e:
				case <-done3:
					return
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		if len(setVecs) == 0 {
			return
		}
		for pair := range server.seti.lsh.QueryPlus(setSigs, done4) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairJaccardPlus(tableID, server.seti.domainDir, columnIndex, pair.QueryIndex, server.seti.numHash, setVecs, setCards[pair.QueryIndex])
			//e.Percentile = getPercentile(server.attCDFs["set"], e.Sim)
			e.Percentile = opendata.GetPerturbedPercentile(server.attCDFs["set"], e.Sim, server.perturbationDelta)
			//e.Measure = "set"
			if e.Percentile.Value != 0.0 {
				//reduceBatch <- e
				select {
				case reduceBatch <- e:
				case <-done4:
					return
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		if len(noOntVecs) == 0 {
			return
		}
		/*
			for pair := range server.semseti.lsh.QueryPlus(noOntSigs, done1) {
				tableID, columnIndex := fromColumnID(pair.CandidateKey)
				e := getColumnPairOntJaccardPlus(tableID, server.semseti.domainDir, columnIndex, pair.QueryIndex, server.semseti.numHash, ontVecs[pair.QueryIndex], noOntVecs[pair.QueryIndex], ontCards[pair.QueryIndex], noOntCards[pair.QueryIndex])
				p2 := opendata.GetPerturbedPercentile(server.attCDFs["semset"], e.Sim, server.perturbationDelta)
				if p2.Value != 0.0 {
					e.Percentile = p2
					//e.Measure = "semset"
					//reduceBatch <- e
					select {
					case reduceBatch <- e:
					case <-done1:
						return
					}
				}
			}
		*/
	}()
	go func() {
		defer wg.Done()
		if len(ontVecs) == 0 {
			return
		}
		/*
		for pair := range server.semi.lsh.QueryPlus(ontSigs, done2) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.semi.domainDir, columnIndex, pair.QueryIndex, server.semi.numHash, ontVecs[pair.QueryIndex], noOntVecs[pair.QueryIndex], ontCards[pair.QueryIndex], noOntCards[pair.QueryIndex])
			//p1 := getPercentile(server.attCDFs["sem"], e.Sim)
			//e := getColumnPairSem(tableID, server.semi.domainDir, columnIndex, pair.QueryIndex, server.semi.numHash, ontVecs[pair.QueryIndex], noOntVecs[pair.QueryIndex], ontCards[pair.QueryIndex], noOntCards[pair.QueryIndex])
			//p1 := opendata.GetPerturbedPercentile(server.attCDFs["sem"], e.Sim, server.perturbationDelta)
			//if p1.Value != 0.0 {
			//	//reduceBatch <- e
			//	select {
			//	case reduceBatch <- e:
			//	case <-done2:
			//		return
			//	}
			//}
			/*
				if e.Sim != e.OntologyHypergeometric {
					p1 := opendata.GetPerturbedPercentile(server.attCDFs["sem"], e.Sim, server.perturbationDelta)
					if p1.Value != 0.0 {
						e.Percentile = p1
						//e.Measure = "sem"
						//reduceBatch <- e
						select {
						case reduceBatch <- e:
						case <-done2:
							return
						}
					}
					//p2 := getPercentile(server.attCDFs["semset"], e.Sim)
					p2 := opendata.GetPerturbedPercentile(server.attCDFs["semset"], e.Sim, server.perturbationDelta)

					if p2.Value != 0.0 {
						e.Percentile = p2
						//e.Measure = "semset"
						//reduceBatch <- e
						select {
						case reduceBatch <- e:
						case <-done2:
							return
						}
					}
				} else {
			*/
			p1 := opendata.GetPerturbedPercentile(server.attCDFs["sem"], e.Sim, server.perturbationDelta)
			if p1.Value != 0.0 {
				e.Percentile = p1
				//e.Measure = "sem"
				//reduceBatch <- e
				select {
				case reduceBatch <- e:
				case <-done2:
					return
				}
			}
			//}
		}
		*/
	}()
	wwg := &sync.WaitGroup{}
	wwg.Add(1)
	go func() {
		for pair := range reduceBatch {
			if alignment.hasCompleted(pair.CandTableID) {
				continue
			}
			//reduceQueue.Push(pair, pair.Percentile)
			reduceQueue.Push(pair, pair.Percentile.ValueMinus, pair.Percentile.ValuePlus)
			//reduceQueue.Push(pair, pair.Percentile.Value, pair.Percentile.Value)
			if reduceQueue.Size() == batchSize {
				// checking if we have processed too many batches
				if numBatches > 2 {
					log.Printf("enough searching")
					close(done1)
					close(done2)
					close(done3)
					close(done4)
					wwg.Done()
					return
				}
				log.Printf("numBatches: %d", numBatches)
				numBatches += 1
				if finished := alignment.processPairsCombined(reduceQueue, results, queryTableID); finished {
					close(done1)
					close(done2)
					close(done3)
					close(done4)
					wwg.Done()
					return
				}
				//reduceQueue = pqueue.NewTopKQueue(batchSize)
				reduceQueue = pqueuespan.NewTopKQueue(batchSize)
			}
		}
		if reduceQueue.Size() != 0 {
			alignment.processPairsCombined(reduceQueue, results, queryTableID)
		} //
		close(done1)
		close(done2)
		close(done3)
		close(done4)
		//}
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

//func (a alignment) processPairsCombined(reduceQueue *pqueue.TopKQueue, out chan<- SearchResult, queryTableID string) bool {
func (a alignment) processPairsCombined(reduceQueue *pqueuespan.TopKQueue, out chan<- SearchResult, queryTableID string) bool {
	//cAlignmentQueue := pqueue.NewTopKQueue(a.n)
	cAlignmentQueue := pqueuespan.NewTopKQueue(a.n)
	alignedTables := make(chan SearchResult)
	tablesToAlign := make(chan string)
	wg := &sync.WaitGroup{}
	wg.Add(a.n + 1)
	go func() {
		//wg.Add(1)
		defer wg.Done()
		defer close(tablesToAlign)
		pairs, _, _ := reduceQueue.Descending()
		for i := range pairs {
			pair := pairs[i].(Pair)
			if a.hasCompleted(pair.CandTableID) {
				continue
			}
			tablesToAlign <- pair.CandTableID
			a.completedTables.Update(pair.CandTableID)
			if a.completedTables.Unique() == a.n {
				return
			}
		}
	}()
	//wg.Add(a.n)
	for i := 0; i < a.n; i++ {
		go func(int) {
			for tp := range tablesToAlign {
				candTableID := tp
				cAlignment := alignTables(queryTableID, candTableID, a.domainDir, a.attCDFs, a.tableCDF, a.perturbationDelta)
				result := SearchResult{
					CandidateTableID:         candTableID,
					Alignment:                cAlignment.alignment,
					K:                        len(cAlignment.percentiles),
					N:                        i,
					Duration:                 float64(time.Now().Sub(a.startTime)) / float64(1000000),
					CUnionabilityScores:      cAlignment.scores,
					CUnionabilityPercentiles: cAlignment.percentiles,
					MaxC:                     cAlignment.maxC,
					BestC:                    cAlignment.bestC,
					SketchedQueryColsNum:     cAlignment.sketchedQueryColsNum,
					SketchedCandidateColsNum: cAlignment.sketchedQueryColsNum,
				}
				alignedTables <- result
			}
			wg.Done()
		}(i)
	}
	wwg := &sync.WaitGroup{}
	wwg.Add(1)
	go func() {
		for result := range alignedTables {
			//cAlignmentQueue.Push(result, result.CUnionabilityPercentiles[result.BestC-1])
			if result.CUnionabilityPercentiles[result.BestC-1].Value != 0.0 {
				cAlignmentQueue.Push(result, result.CUnionabilityPercentiles[result.BestC-1].ValueMinus, result.CUnionabilityPercentiles[result.BestC-1].ValuePlus)
			}
			//cAlignmentQueue.Push(result, result.CUnionabilityPercentiles[result.BestC-1].Value, result.CUnionabilityPercentiles[result.BestC-1].Value)
			//cAlignmentQueue.Push(result, result.CUnionabilityPercentiles[result.BestC-1].ValuePlus, result.CUnionabilityPercentiles[result.BestC-1].ValuePlus)
			//cAlignmentQueue.Push(result, result.CUnionabilityPercentiles[result.BestC-1].ValueMinus, result.CUnionabilityPercentiles[result.BestC-1].ValueMinus)
			// FN
			//for ci := 0; ci < len(result.CUnionabilityPercentiles); ci++ {
			//	r := result
			//	r.C = ci + 1
			//	cAlignmentQueue.Push(r, r.CUnionabilityPercentiles[ci].Value, r.CUnionabilityPercentiles[ci].Value)
			//}
			// end FN
		}
		//seen := make(map[string]bool)
		results, _, _ := cAlignmentQueue.Descending()
		for i := range results {
			result := results[i].(SearchResult)
			// FN
			//if _, ok := seen[result.CandidateTableID]; !ok {
			result.Duration = float64(time.Now().Sub(a.startTime)) / float64(1000000)
			result.N = i
			//	seen[result.CandidateTableID] = true
			out <- result
			//}
		}
		wwg.Done()
	}()
	wg.Wait()
	close(alignedTables)
	wwg.Wait()
	return a.completedTables.Unique() == a.n
}
