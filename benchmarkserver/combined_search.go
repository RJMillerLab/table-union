package benchmarkserver

import (
	"log"
	"math"
	"sync"
	"time"

	"github.com/ekzhu/counter"
	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/minhashlsh"
	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/pqueue"
)

type tablePair struct {
	queryTable     string
	candidateTable string
}

func initCAlignment(N int, tableCDF map[int]opendata.CDF, setCDF, semCDF, semsetCDF, nlCDF opendata.CDF, domainDir string) alignment {
	return alignment{
		completedTables: counter.NewCounter(),
		partialAlign:    make(map[string](*counter.Counter)),
		reverseAlign:    make(map[string](*counter.Counter)),
		tableQueues:     make(map[string]*pqueue.TopKQueue),
		n:               N,
		startTime:       time.Now(),
		tableCDF:        tableCDF,
		setCDF:          setCDF,
		semCDF:          semCDF,
		semsetCDF:       semsetCDF,
		nlCDF:           nlCDF,
		domainDir:       domainDir,
	}
}

func (server *CombinedServer) CombinedOrderAll(nlMeans, nlCovars [][]float64, setVecs, noOntVecs, ontVecs [][]uint64, N int, noOntCards, ontCards, nlCards, setCards []int, queryTableID string) <-chan SearchResult {
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
	alignment := initCAlignment(N, server.tableCDF, server.setCDF, server.semCDF, server.semsetCDF, server.nlCDF, server.seti.domainDir)
	reduceQueue := pqueue.NewTopKQueue(batchSize)
	reduceBatch := make(chan Pair)
	done1 := make(chan struct{})
	done2 := make(chan struct{})
	done3 := make(chan struct{})
	done4 := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(4)
	go func() {
		for pair := range server.nli.lsh.QueryPlus(nlMeans, done3) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairPlus(tableID, server.seti.domainDir, columnIndex, pair.QueryIndex, nlMeans[pair.QueryIndex], nlCovars[pair.QueryIndex], nlCards[pair.QueryIndex])
			if !math.IsNaN(e.T2) && !math.IsNaN(e.F) && !math.IsInf(e.T2, 0) && !math.IsInf(e.F, 0) {
				if e.Sim != 0.0 {
					e.Percentile = getPercentile(server.nlCDF, e.Sim)
					reduceBatch <- e
				}
			}
		}
		wg.Done()
	}()
	go func() {
		for pair := range server.seti.lsh.QueryPlus(setSigs, done4) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairJaccardPlus(tableID, server.seti.domainDir, columnIndex, pair.QueryIndex, server.seti.numHash, setVecs, setCards[pair.QueryIndex])
			if e.Sim != 0.0 {
				e.Percentile = getPercentile(server.setCDF, e.Sim)
				reduceBatch <- e
			}
		}
	}()
	go func() {
		for pair := range server.semseti.lsh.QueryPlus(noOntSigs, done1) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.semseti.domainDir, columnIndex, pair.QueryIndex, server.semseti.numHash, ontVecs[pair.QueryIndex], noOntVecs[pair.QueryIndex], ontCards[pair.QueryIndex], noOntCards[pair.QueryIndex])
			if e.Sim != 0.0 {
				e.Percentile = getPercentile(server.semsetCDF, e.Sim)
				if e.Percentile != 0.0 {
					reduceBatch <- e
				}
			}
		}
		wg.Done()
	}()
	go func() {
		for pair := range server.semi.lsh.QueryPlus(ontSigs, done2) {
			tableID, columnIndex := fromColumnID(pair.CandidateKey)
			e := getColumnPairOntJaccardPlus(tableID, server.semi.domainDir, columnIndex, pair.QueryIndex, server.semi.numHash, ontVecs[pair.QueryIndex], noOntVecs[pair.QueryIndex], ontCards[pair.QueryIndex], noOntCards[pair.QueryIndex])
			if e.Sim != 0.0 {
				p1 := getPercentile(server.semCDF, e.Sim)
				p2 := getPercentile(server.semsetCDF, e.Sim)
				if p1 > p2 {
					e.Percentile = p1
				} else {
					e.Percentile = p2
				}
				if e.Percentile != 0.0 {
					reduceBatch <- e
				}
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
				if finished := alignment.processPairsCombined(reduceQueue, results, queryTableID); finished {
					close(done1)
					close(done2)
					close(done3)
					close(done4)
					wwg.Done()
					return
				}
				reduceQueue = pqueue.NewTopKQueue(batchSize)
			}
		}
		if reduceQueue.Size() != 0 {
			alignment.processPairsCombined(reduceQueue, results, queryTableID)
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

func (a alignment) processPairsCombined(reduceQueue *pqueue.TopKQueue, out chan<- SearchResult, queryTableID string) bool {
	cAlignmentQueue := pqueue.NewTopKQueue(a.n)
	alignedTables := make(chan SearchResult)
	tablesToAlign := make(chan string)
	wg := &sync.WaitGroup{}
	wg.Add(a.n + 1)
	go func() {
		defer wg.Done()
		defer close(tablesToAlign)
		pairs, _ := reduceQueue.Descending()
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
		go func() {
			for tp := range tablesToAlign {
				candTableID := tp
				cAlignment := alignTables(queryTableID, candTableID, a.domainDir, a.setCDF, a.semCDF, a.semsetCDF, a.nlCDF, a.tableCDF)
				result := SearchResult{
					CandidateTableID:         candTableID,
					Alignment:                cAlignment.alignment,
					K:                        len(cAlignment.percentiles),
					Duration:                 float64(time.Now().Sub(a.startTime)) / float64(1000000),
					CUnionabilityScores:      cAlignment.scores,
					CUnionabilityPercentiles: cAlignment.percentiles,
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
			cAlignmentQueue.Push(result, result.CUnionabilityPercentiles[result.BestC-1])
		}
		results, _ := cAlignmentQueue.Descending()
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
