package benchmarkserver

import (
	"log"
	"strconv"
	"time"

	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/pqueue"
	"github.com/ekzhu/counter"
	"github.com/fnargesian/pqueuespan"
)

type alignment struct {
	completedTables *counter.Counter
	partialAlign    map[string](*counter.Counter)
	reverseAlign    map[string](*counter.Counter)
	tableQueues     map[string](*pqueue.TopKQueue)
	tableSpanQueues map[string](*pqueuespan.TopKQueue)
	k               int
	n               int
	startTime       time.Time
	tableCDF        map[int]opendata.CDF
	//setCDF          opendata.CDF
	//semCDF          opendata.CDF
	//semsetCDF       opendata.CDF
	//nlCDF           opendata.CDF
	attCDFs           map[string]opendata.CDF
	domainDir         string
	perturbationDelta float64
}

type embDomain struct {
	columnIndex int
	sumVec      []float64
}

type edge struct {
	srcIndex  int
	destIndex int
}

type Pair struct {
	CandTableID            string
	CandColIndex           int
	QueryColIndex          int
	QueryCardinality       int
	CandCardinality        int
	Jaccard                float64
	Containment            float64
	Hypergeometric         float64
	OntologyJaccard        float64
	OntologyHypergeometric float64
	SemSet                 float64
	Cosine                 float64
	F                      float64
	T2                     float64
	Sim                    float64
	Percentile             opendata.Percentile //between 0 and 1
	Measure                string
}

type CUnionableVector struct {
	queryTable     string
	candidateTable string
	scores         []float64
	percentiles    []opendata.Percentile
	alignment      []Pair
	maxC           int
	bestC          int
}

//func alignTables(queryTable, candidateTable, domainDir string, setCDF, semCDF, semsetCDF, nlCDF opendata.CDF, tableCDF map[int]opendata.CDF) CUnionableVector {
func alignTables(queryTable, candidateTable, domainDir string, attCDFs map[string]opendata.CDF, tableCDF map[int]opendata.CDF, perturbationDelta float64) CUnionableVector {
	log.Printf("processing candidate table %s.", candidateTable)
	var result CUnionableVector
	cUnionabilityScores := make([]float64, 0)
	cUnionabilityPercentiles := make([]opendata.Percentile, 0)
	queryTextDomains := getTextDomains(queryTable, domainDir)
	candTextDomains := getTextDomains(candidateTable, domainDir)
	partialAlign := counter.NewCounter()
	reverseAlign := counter.NewCounter()
	maxC := len(candTextDomains)
	alignment := make([]Pair, 0)
	batch := pqueuespan.NewTopKQueue(len(queryTextDomains) * len(candTextDomains))
	for _, qindex := range queryTextDomains {
		for _, cindex := range candTextDomains {
			p := getAttUnionabilityPair(queryTable, candidateTable, qindex, cindex, attCDFs, perturbationDelta)
			if p.Percentile.Value != 0.0 {
				//batch.Push(p, p.Percentile)
				batch.Push(p, p.Percentile.ValueMinus, p.Percentile.ValuePlus)
			}
		}
	}

	pairs, _, _ := batch.Descending()
	for i := range pairs {
		pair := pairs[i].(Pair)
		if partialAlign.Has(strconv.Itoa(pair.CandColIndex)) || reverseAlign.Has(strconv.Itoa(pair.QueryColIndex)) {
			// because we are using priority queue
			continue
		}
		partialAlign.Update(strconv.Itoa(pair.CandColIndex))
		reverseAlign.Update(strconv.Itoa(pair.QueryColIndex))
		alignment = append(alignment, pair)
		if len(cUnionabilityScores) == 0 {
			cUnionabilityScores = append(cUnionabilityScores, pair.Percentile.Value)
		} else {
			cUnionabilityScores = append(cUnionabilityScores, cUnionabilityScores[len(cUnionabilityScores)-1]*pair.Percentile.Value)
		}
		cUnionabilityPercentiles = append(cUnionabilityPercentiles, opendata.GetPerturbedPercentile(tableCDF[len(cUnionabilityPercentiles)+1], cUnionabilityScores[len(cUnionabilityScores)-1], perturbationDelta))
		//cUnionabilityPercentiles = append(cUnionabilityPercentiles, getPercentile(tableCDF[len(cUnionabilityPercentiles)+1], cUnionabilityScores[len(cUnionabilityScores)-1]))
		// When we get c unique column alignments for a candidate table
		if partialAlign.Unique() == maxC {
			break
		}
	}
	maxC = len(cUnionabilityPercentiles)
	// if no alignment found for k = number of query columns
	for i := len(cUnionabilityScores); i < maxC; i += 1 {
		cUnionabilityScores = append(cUnionabilityScores, 0.0)
		p := opendata.Percentile{0.0, 0.0, 0.0, 0.0}
		cUnionabilityPercentiles = append(cUnionabilityPercentiles, p)
	}
	//cUP := make([]opendata.Percentile, len(cUnionabilityPercentiles))
	//copy(cUP, cUnionabilityPercentiles)
	//s := cUP
	//inds := make([]int, len(s))
	// ascending sort
	//floats.Argsort(s, inds)
	s, bestC := opendata.SortPercentiles(cUnionabilityPercentiles)
	result = CUnionableVector{
		queryTable:     queryTable,
		candidateTable: candidateTable,
		scores:         cUnionabilityScores,
		//percentiles:    cUnionabilityPercentiles,
		percentiles: s,
		alignment:   alignment,
		maxC:        maxC,
		//bestC:       inds[len(inds)-1] + 1,
		bestC: bestC + 1,
	}
	return result
}

func getAttUnionabilityPair(queryTable, candidateTable string, qindex, cindex int, attCDFs map[string]opendata.CDF, perturbationDelta float64) Pair {
	uScore, uPercentile, uMeasures := opendata.GetAttUnionabilityPercentile(queryTable, candidateTable, qindex, cindex, attCDFs, perturbationDelta)
	p := Pair{
		CandTableID:   candidateTable,
		CandColIndex:  cindex,
		QueryColIndex: qindex,
		Sim:           uScore,
		Percentile:    uPercentile,
		Measure:       uMeasures[0],
	}
	for _, m := range uMeasures {
		if m == "set" {
			p.Hypergeometric = uScore
		}
		if m == "sem" {
			p.OntologyHypergeometric = uScore
		}
		if m == "semset" {
			p.SemSet = uScore
		}
		if m == "nl" {
			p.Cosine = uScore
		}
	}
	return p
}
