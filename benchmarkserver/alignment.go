package benchmarkserver

import (
	"log"
	"strconv"
	"time"

	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/pqueue"
	"github.com/ekzhu/counter"
	"github.com/gonum/floats"
)

type alignment struct {
	completedTables *counter.Counter
	partialAlign    map[string](*counter.Counter)
	reverseAlign    map[string](*counter.Counter)
	tableQueues     map[string](*pqueue.TopKQueue)
	k               int
	n               int
	startTime       time.Time
	tableCDF        map[int]opendata.CDF
	setCDF          opendata.CDF
	semCDF          opendata.CDF
	semsetCDF       opendata.CDF
	nlCDF           opendata.CDF
	domainDir       string
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
	Percentile             float64 //between 0 and 1
}

type CUnionableVector struct {
	queryTable     string
	candidateTable string
	scores         []float64
	percentiles    []float64
	alignment      []Pair
	bestC          int
}

func alignTables(queryTable, candidateTable, domainDir string, setCDF, semCDF, semsetCDF, nlCDF opendata.CDF, tableCDF map[int]opendata.CDF) CUnionableVector {
	log.Printf("processing candidate table %s.", candidateTable)
	var result CUnionableVector
	cUnionabilityScores := make([]float64, 0)
	cUnionabilityPercentiles := make([]float64, 0)
	queryTextDomains := getTextDomains(queryTable, domainDir)
	candTextDomains := getTextDomains(candidateTable, domainDir)
	partialAlign := counter.NewCounter()
	reverseAlign := counter.NewCounter()
	maxC := len(candTextDomains)
	alignment := make([]Pair, 0)
	batch := pqueue.NewTopKQueue(len(queryTextDomains) * len(candTextDomains))
	for _, qindex := range queryTextDomains {
		for _, cindex := range candTextDomains {
			p := getAttUnionabilityPair(queryTable, candidateTable, qindex, cindex, setCDF, semCDF, semsetCDF, nlCDF)
			if p.Sim != 0.0 {
				batch.Push(p, p.Sim)
			}
		}
	}

	pairs, _ := batch.Descending()
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
			cUnionabilityScores = append(cUnionabilityScores, pair.Sim)
		} else {
			cUnionabilityScores = append(cUnionabilityScores, cUnionabilityScores[len(cUnionabilityScores)-1]*pair.Sim)
		}
		cUnionabilityPercentiles = append(cUnionabilityPercentiles, getPercentile(tableCDF[len(cUnionabilityPercentiles)+1], cUnionabilityScores[len(cUnionabilityScores)-1]))
		// When we get c unique column alignments for a candidate table
		if partialAlign.Unique() == maxC {
			break
		}
	}
	// if no alignment found for k = number of query columns
	for i := len(cUnionabilityScores); i < maxC; i += 1 {
		cUnionabilityScores = append(cUnionabilityScores, 0.0)
		cUnionabilityPercentiles = append(cUnionabilityPercentiles, 0.0)
	}
	cUP := make([]float64, len(cUnionabilityPercentiles))
	copy(cUP, cUnionabilityPercentiles)
	s := cUP
	inds := make([]int, len(s))
	floats.Argsort(s, inds)
	result = CUnionableVector{
		queryTable:     queryTable,
		candidateTable: candidateTable,
		scores:         cUnionabilityScores,
		percentiles:    cUnionabilityPercentiles,
		alignment:      alignment,
		bestC:          inds[len(cUnionabilityPercentiles)-1] + 1,
	}
	return result
}

func getAttUnionabilityPair(queryTable, candidateTable string, qindex, cindex int, setCDF, semCDF, semsetCDF, nlCDF opendata.CDF) Pair {
	uScore, uMeasures := opendata.GetAttUnionabilityPercentile(queryTable, candidateTable, qindex, cindex, setCDF, semCDF, semsetCDF, nlCDF)
	p := Pair{
		CandTableID:   candidateTable,
		CandColIndex:  cindex,
		QueryColIndex: qindex,
		Sim:           uScore,
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
