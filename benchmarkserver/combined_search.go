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

//func initCAlignment(N int, tableCDF map[int]opendata.CDF, setCDF, semCDF, semsetCDF, nlCDF opendata.CDF, domainDir string) alignment {
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
	//alignment := initCAlignment(N, server.tableCDF, server.setCDF, server.semCDF, server.semsetCDF, server.nlCDF, server.seti.domainDir)
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
		if len(nlMeans) == 0 {
			return
		}
		for pair := range server.nli.lsh.QueryPlus(nlMeans, done3) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairPlus(tableID, server.seti.domainDir, columnIndex, pair.QueryIndex, nlMeans[pair.QueryIndex], nlCovars[pair.QueryIndex], nlCards[pair.QueryIndex])
			//e.Percentile = getPercentile(server.attCDFs["nl"], e.Sim)
			e.Percentile = opendata.GetPerturbedPercentile(server.attCDFs["nl"], e.Sim, server.perturbationDelta)
			if e.Percentile.Value != 0.0 {
				reduceBatch <- e
			}
		}
		wg.Done()
	}()
	go func() {
		if len(setVecs) == 0 {
			return
		}
		for pair := range server.seti.lsh.QueryPlus(setSigs, done4) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairJaccardPlus(tableID, server.seti.domainDir, columnIndex, pair.QueryIndex, server.seti.numHash, setVecs, setCards[pair.QueryIndex])
			//e.Percentile = getPercentile(server.attCDFs["set"], e.Sim)
			e.Percentile = opendata.GetPerturbedPercentile(server.attCDFs["set"], e.Sim, server.perturbationDelta)
			if e.Percentile.Value != 0.0 {
				reduceBatch <- e
			}
		}
	}()
	go func() {
		if len(noOntVecs) == 0 {
			return
		}
		for pair := range server.semseti.lsh.QueryPlus(noOntSigs, done1) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.semseti.domainDir, columnIndex, pair.QueryIndex, server.semseti.numHash, ontVecs[pair.QueryIndex], noOntVecs[pair.QueryIndex], ontCards[pair.QueryIndex], noOntCards[pair.QueryIndex])
			//p1 := getPercentile(server.attCDFs["sem"], e.OntologyHypergeometric)
			//p2 := getPercentile(server.attCDFs["semset"], e.Sim)
			p1 := opendata.GetPerturbedPercentile(server.attCDFs["sem"], e.Sim, server.perturbationDelta)
			p2 := opendata.GetPerturbedPercentile(server.attCDFs["semset"], e.Sim, server.perturbationDelta)
			//if p1 > 0.9 && e.OntologyHypergeometric < 0.4 {
			//	log.Printf("%s.%d has sem sim %f and perc %f and with 0.1 pert becomes [%f, %f].", tableID, columnIndex, e.OntologyHypergeometric, p1, getPercentile(server.attCDFs["sem"], math.Max(e.OntologyHypergeometric-0.1, 0.0)), getPercentile(server.attCDFs["sem"], math.Min(e.OntologyHypergeometric+0.1, 1.0)))
			//}
			//if p2 > 0.9 && e.Sim < 0.4 {
			//	log.Printf("%s.%d has semset sim %f and perc %f and with 0.1 pert becomes [%f, %f].", tableID, columnIndex, e.Sim, p2, getPercentile(server.attCDFs["semset"], math.Max(e.Sim-0.1, 0.0)), getPercentile(server.attCDFs["semset"], math.Min(e.Sim+0.1, 1.0)))
			//}
			cmp := opendata.ComparePercentiles(p1, p2)
			//if p1 > p2 {
			if cmp == 1 {
				e.Percentile = p1
				e.Measure = "sem"
				reduceBatch <- e
				//} else if p1 < p2 {
			} else if cmp == -1 {
				e.Percentile = p2
				e.Measure = "semset"
				reduceBatch <- e
			} else if p1.Value != 0.0 && p2.Value != 0.0 {
				e.Percentile = p2
				e.Measure = "semset"
				reduceBatch <- e
				e.Percentile = p1
				e.Measure = "sem"
				reduceBatch <- e
			}
		}
		wg.Done()
	}()
	go func() {
		if len(ontVecs) == 0 {
			return
		}
		for pair := range server.semi.lsh.QueryPlus(ontSigs, done2) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.semi.domainDir, columnIndex, pair.QueryIndex, server.semi.numHash, ontVecs[pair.QueryIndex], noOntVecs[pair.QueryIndex], ontCards[pair.QueryIndex], noOntCards[pair.QueryIndex])
			//p1 := getPercentile(server.attCDFs["sem"], e.Sim)
			p1 := opendata.GetPerturbedPercentile(server.attCDFs["sem"], e.Sim, server.perturbationDelta)
			//p2 := getPercentile(server.attCDFs["semset"], e.Sim)
			p2 := opendata.GetPerturbedPercentile(server.attCDFs["semset"], e.Sim, server.perturbationDelta)
			cmp := opendata.ComparePercentiles(p1, p2)
			if cmp == 1 {
				//if p1 > p2 {
				e.Percentile = p1
				e.Measure = "sem"
				reduceBatch <- e
				//} else if p1 < p2 {
			} else if cmp == -1 {
				e.Percentile = p2
				e.Measure = "semset"
				reduceBatch <- e
			} else if p1.Value != 0.0 && p2.Value != 0.0 {
				e.Percentile = p2
				e.Measure = "semset"
				reduceBatch <- e
				e.Percentile = p1
				e.Measure = "sem"
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
			//reduceQueue.Push(pair, pair.Percentile)
			reduceQueue.Push(pair, pair.Percentile.ValueMinus, pair.Percentile.ValuePlus)
			if reduceQueue.Size() == batchSize {
				// checking if we have processed too many batches
				if numBatches > 3 {
					close(done1)
					close(done2)
					close(done3)
					close(done4)
					wwg.Done()
					return
				}
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
			close(done1)
			close(done2)
			close(done3)
			close(done4)
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

//func (a alignment) processPairsCombined(reduceQueue *pqueue.TopKQueue, out chan<- SearchResult, queryTableID string) bool {
func (a alignment) processPairsCombined(reduceQueue *pqueuespan.TopKQueue, out chan<- SearchResult, queryTableID string) bool {
	//cAlignmentQueue := pqueue.NewTopKQueue(a.n)
	cAlignmentQueue := pqueuespan.NewTopKQueue(a.n)
	alignedTables := make(chan SearchResult)
	tablesToAlign := make(chan string)
	wg := &sync.WaitGroup{}
	//wg.Add(a.n + 1)
	go func() {
		wg.Add(1)
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
	for i := 0; i < a.n; i++ {
		wg.Add(1)
		go func() {
			for tp := range tablesToAlign {
				candTableID := tp
				//cAlignment := alignTables(queryTableID, candTableID, a.domainDir, a.setCDF, a.semCDF, a.semsetCDF, a.nlCDF, a.tableCDF)
				cAlignment := alignTables(queryTableID, candTableID, a.domainDir, a.attCDFs, a.tableCDF, a.perturbationDelta)
				result := SearchResult{
					CandidateTableID:         candTableID,
					Alignment:                cAlignment.alignment,
					K:                        len(cAlignment.percentiles),
					Duration:                 float64(time.Now().Sub(a.startTime)) / float64(1000000),
					CUnionabilityScores:      cAlignment.scores,
					CUnionabilityPercentiles: cAlignment.percentiles,
					MaxC:  cAlignment.maxC,
					BestC: cAlignment.bestC,
				}
				alignedTables <- result
			}
			wg.Done()
		}()
	}
	wwg := &sync.WaitGroup{}
	wwg.Add(1)
	go func() {
		for result := range alignedTables {
			// this scoring can change
			//cAlignmentQueue.Push(result, result.CUnionabilityPercentiles[result.BestC-1])
			//lb, ub := perturbPercentile(a.tableCDF[result.BestC], result.CUnionabilityPercentiles[result.BestC-1], delta)
			//pair.PercentilePlus = ub
			//pair.PercentileMinus = lb
			cAlignmentQueue.Push(result, result.CUnionabilityPercentiles[result.BestC-1].ValueMinus, result.CUnionabilityPercentiles[result.BestC-1].ValuePlus)
		}
		results, _, _ := cAlignmentQueue.Descending()
		for i := range results {
			result := results[i].(SearchResult)
			result.Duration = float64(time.Now().Sub(a.startTime)) / float64(1000000)
			out <- result
		}
		wwg.Done()
	}()
	wg.Wait()
	close(alignedTables)
	wwg.Wait()
	return a.completedTables.Unique() == a.n
}
