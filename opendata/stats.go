package opendata

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/RJMillerLab/table-union/embedding"
)

var (
	ByteOrder = binary.BigEndian
)

type AttributeUnion struct {
	queryTable  string
	candTable   string
	queryColumn int
	candColumn  int
	score       float64
	measure     []string
}

type TableUnion struct {
	queryTable string
	candTable  string
	alignment  []AttributeUnion
	score      float64
}

func ComputeAttUnionabilityScores(queryTable, candidateTable string) ([]AttributeUnion, int, int) {
	union := make([]AttributeUnion, 0)
	queryTextDomains := getTextDomains(queryTable)
	candTextDomains := getTextDomains(candidateTable)
	queryColNum := len(queryTextDomains)
	candColNum := len(candTextDomains)
	for _, qindex := range queryTextDomains {
		for _, cindex := range candTextDomains {
			score, measure := getAttUnionability(queryTable, candidateTable, qindex, cindex)
			attunion := AttributeUnion{
				queryTable:  queryTable,
				candTable:   candidateTable,
				queryColumn: qindex,
				candColumn:  cindex,
				score:       score,
				measure:     measure,
			}
			union = append(union, attunion)
		}

	}
	return union, queryColNum, candColNum
}

func ComputeTableUnionability(queryTable, candTable string, attunions []AttributeUnion, queryColNum, candColNum int) TableUnion {
	bestAlignment := make([]AttributeUnion, 0)
	scores := make([]float64, 0)
	for _, au := range attunions {
		scores = append(scores, au.score)
	}
	s := NewSlice(scores...)
	sort.Sort(s)
	m := int(math.Min(float64(queryColNum), float64(candColNum)))
	var matchNum int
	covered := make(map[string]bool)
	for _, ix := range s.idx {
		qIndex := attunions[ix].queryColumn
		cIndex := attunions[ix].candColumn
		if attunions[ix].score == 0.0 {
			continue
		}
		if _, ok := covered[queryTable+string(qIndex)]; !ok {
			if _, ok := covered[candTable+string(cIndex)]; !ok {
				bestAlignment = append(bestAlignment, attunions[ix])
				matchNum += 1
				covered[queryTable+string(qIndex)] = true
				covered[candTable+string(cIndex)] = true
			}
		}

		if matchNum == m {
			break
		}
	}
	return TableUnion{
		queryTable: queryTable,
		candTable:  candTable,
		alignment:  bestAlignment,
	}
}

func getAttUnionability(queryTable, candidateTable string, queryIndex, candIndex int) (float64, []string) {
	var uScore float64
	uMeasure := make([]string, 0)
	uSet := setUnionability(queryTable, candidateTable, queryIndex, candIndex)
	uScore = uSet
	uMeasure = append(uMeasure, "set")
	uNL := nlUnionability(queryTable, candidateTable, queryIndex, candIndex)
	if uNL > uScore {
		uScore = uNL
		uMeasure = make([]string, 0)
		uMeasure = append(uMeasure, "nl")
	} else if uNL == uScore {
		uMeasure = append(uMeasure, "nl")
	}
	uSem, uSemSet := semSetUnionability(queryTable, candidateTable, queryIndex, candIndex)
	if uSemSet > uScore {
		uScore = uSemSet
		uMeasure = make([]string, 0)
		uMeasure = append(uMeasure, "semset")
	} else if uSemSet == uScore {
		uMeasure = append(uMeasure, "semset")
	}
	if uSem > uScore {
		uScore = uSem
		uMeasure = make([]string, 0)
		uMeasure = append(uMeasure, "sem")
	} else if uSem == uScore {
		uMeasure = append(uMeasure, "sem")
	}
	return uScore, uMeasure
}

func semUnionability(queryTable, candidateTable string, queryIndex, candIndex int) float64 {
	ontMinhashFilename := getOntMinhashFilename(candidateTable, candIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return 0.0
	}
	coVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return 0.0
	}
	ontMinhashFilename = getOntMinhashFilename(queryTable, queryIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return 0.0
	}
	qoVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return 0.0
	}
	ontJaccard := estimateJaccard(coVec, qoVec)
	_, nA := getOntDomainCardinality(candidateTable, candIndex)
	_, nB := getOntDomainCardinality(queryTable, queryIndex)
	ontProb := sameDomainProb(ontJaccard, nA, nB)
	return ontProb
}

func semSetUnionability(queryTable, candidateTable string, queryIndex, candIndex int) (float64, float64) {
	minhashFilename := getUnannotatedMinhashFilename(candidateTable, candIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		return 0.0, 0.0
	}
	cuaVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		return 0.0, 0.0
	}
	minhashFilename = getUnannotatedMinhashFilename(queryTable, queryIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		return 0.0, 0.0
	}
	quaVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		return 0.0, 0.0
	}
	jaccard := estimateJaccard(quaVec, cuaVec)
	// computing ontology jaccard
	ontMinhashFilename := getOntMinhashFilename(candidateTable, candIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return 0.0, 0.0
	}
	coVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return 0.0, 0.0
	}
	ontMinhashFilename = getOntMinhashFilename(queryTable, queryIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return 0.0, 0.0
	}
	qoVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return 0.0, 0.0
	}
	ontJaccard := estimateJaccard(coVec, qoVec)
	noA, nA := getOntDomainCardinality(candidateTable, candIndex)
	noB, nB := getOntDomainCardinality(queryTable, queryIndex)
	noOntProb := sameDomainProb(jaccard, noA, noB)
	ontProb := sameDomainProb(ontJaccard, nA, nB)
	return ontProb, noOntProb + ontProb - ontProb*noOntProb
}

func nlUnionability(queryTable, candidateTable string, queryIndex, candIndex int) float64 {
	meanFilename := filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-mean", candidateTable, candIndex))
	if _, err := os.Stat(meanFilename); os.IsNotExist(err) {
		log.Printf("Mean embedding file %s does not exist.", meanFilename)
		return -1.0
	}
	cMean, err := embedding.ReadVecFromDisk(meanFilename, ByteOrder)
	if err != nil {
		log.Printf("Error in reading %s from disk.", meanFilename)
		return -1.0
	}
	meanFilename = filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-mean", queryTable, queryIndex))
	if _, err := os.Stat(meanFilename); os.IsNotExist(err) {
		log.Printf("Mean embedding file %s does not exist.", meanFilename)
		return -1.0
	}
	qMean, err := embedding.ReadVecFromDisk(meanFilename, ByteOrder)
	if err != nil {
		log.Printf("Error in reading %s from disk.", meanFilename)
		return -1.0
	}
	// reading covariance matrix
	//covarFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-covar", candTableID, candColIndex))
	//if _, err := os.Stat(covarFilename); os.IsNotExist(err) {
	//  log.Printf("Embedding file %s does not exist.", covarFilename)
	//  panic(err)
	//}
	//covar, err := embedding.ReadVecFromDisk(covarFilename, ByteOrder)
	//if err != nil {
	//  log.Printf("Error in reading %s from disk.", covarFilename)
	//  panic(err)
	//}
	//cCard := getDomainSize(candidateTable, domainDir, candIndex)
	//qCard := getDomainSize(queryTable, domainDir, queryIndex)
	cosine := embedding.Cosine(qMean, cMean)
	//ht2, f := getT2Statistics(mean, queryMean, covar, queryCovar, card, queryCardinality)
	return cosine
}

func setUnionability(queryTable, candidateTable string, queryIndex, candIndex int) float64 {
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candidateTable, candIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("Embedding file %s does not exist.", minhashFilename)
		return -1.0
	}
	cVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", minhashFilename)
		return -1.0
	}
	minhashFilename = getMinhashFilename(queryTable, queryIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("Embedding file %s does not exist.", minhashFilename)
		return -1.0
	}
	qVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", minhashFilename)
		return -1.0
	}
	// inserting the pair into its corresponding priority queue
	jaccard := estimateJaccard(cVec, qVec)
	nB := getDomainCardinality(candidateTable, candIndex)
	nA := getDomainCardinality(queryTable, queryIndex)
	uSet := sameDomainProb(jaccard, nA, nB)
	return uSet
}

func getDomainCardinality(tableID string, index int) int {
	cardpath := path.Join(OutputDir, "domains", tableID)
	cardpath = path.Join(cardpath, fmt.Sprintf("%d.%s", index, "card"))
	f, err := os.Open(cardpath)
	defer f.Close()
	if err != nil {
		return 0.0
	}
	card := 0
	scanner := bufio.NewScanner(f)
	lineIndex := 0
	for scanner.Scan() {
		if lineIndex == 0 {
			c, err := strconv.Atoi(strings.Replace(scanner.Text(), "\n", "", -1))
			if err == nil {
				card = c
			}
			lineIndex += 1
		}
	}
	return card
}

func estimateJaccard(query, candidate []uint64) float64 {
	intersection := 0
	for i := 0; i < len(query); i++ {
		if query[i] == candidate[i] {
			intersection += 1
		}
	}
	return float64(intersection) / float64(len(query))
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

func logHyperGeometricProb(k, K, n, N int) float64 {
	hgp := logCombination(K, k) + logCombination(N-K, n-k) - logCombination(N, n)
	return hgp
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

func getMinhashFilename(tableID string, index int) string {
	fullpath := path.Join(OutputDir, "domains", tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "minhash"))
	return fullpath
}

func getDomainSize(tableID string, index int) int {
	cardpath := path.Join(OutputDir, "domains", tableID)
	cardpath = path.Join(cardpath, fmt.Sprintf("%d.%s", index, "size"))
	f, err := os.Open(cardpath)
	defer f.Close()
	if err != nil {
		return 0.0
	}
	card := 0
	scanner := bufio.NewScanner(f)
	lineIndex := 0
	for scanner.Scan() {
		if lineIndex == 0 {
			c, err := strconv.Atoi(strings.Replace(scanner.Text(), "\n", "", -1))
			if err == nil {
				card = c
			}
			lineIndex += 1
		}
	}
	return card
}

func getUnannotatedMinhashFilename(tableID string, index int) string {
	fullpath := path.Join(OutputDir, "domains", tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "noann-minhash"))
	return fullpath
}

func getOntMinhashFilename(tableID string, index int) string {
	fullpath := path.Join(OutputDir, tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "ont-minhash-l1"))
	//fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "ont-minhash-l2"))
	return fullpath
}

func getOntDomainCardinality(tableID string, index int) (int, int) {
	cardpath := path.Join(OutputDir, "domains", tableID)
	cardpath = path.Join(cardpath, fmt.Sprintf("%d.%s", index, "ont-noann-card"))
	log.Printf("cardpath: %s", cardpath)
	f, err := os.Open(cardpath)
	defer f.Close()
	if err != nil {
		return 0.0, 0.0
	}
	card := 0
	ocard := 0
	scanner := bufio.NewScanner(f)
	lineIndex := 0
	for scanner.Scan() {
		if lineIndex == 0 {
			c, err := strconv.Atoi(strings.Replace(scanner.Text(), "\n", "", -1))
			if err == nil {
				card = c
			}
			lineIndex += 1
		}
		//if lineIndex == 1 {
		//	c, err := strconv.Atoi(strings.Replace(scanner.Text(), "\n", "", -1))
		//	if err == nil {
		//		ocard = c
		//	}
		//	lineIndex += 1
		//}
	}
	ontCardpath := path.Join(OutputDir, "domains", tableID)
	ontCardpath = path.Join(ontCardpath, fmt.Sprintf("%d.%s", index, "ont-card"))
	fo, err := os.Open(ontCardpath)
	defer fo.Close()
	if err != nil {
		return 0.0, 0.0
	}
	scanner = bufio.NewScanner(fo)
	lineIndex = 0
	for scanner.Scan() {
		if lineIndex == 0 {
			c, err := strconv.Atoi(strings.Replace(scanner.Text(), "\n", "", -1))
			if err == nil {
				ocard = c
			} else {
				panic(err)
			}
		}
		lineIndex += 1
	}
	return card, ocard
}

func DoSaveAttScores(scores []AttributeUnion, progress chan ProgressCounter) {
	db, err := sql.Open("sqlite3", AttStatsDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (query_table text,query_column int, candidate_table text,candidate_column int, score real, measure text);`, AttStatsTable, AttStatsTable))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, query_column, candidate_table, candidate_column, score, measure) values(?, ?, ?, ?, ?, ?);`, AttStatsTable))
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for _, score := range scores {
			for _, m := range score.measure {
				_, err = stmt.Exec(score.queryTable, score.queryColumn, score.candTable, score.candColumn, score.score, m)
				if err != nil {
					panic(err)
				}
				progress <- ProgressCounter{1}
			}
		}
		wg.Done()
	}()
	go func() {
		wg.Wait()
		db.Close()
	}()
}

func DoSaveTableScores(union TableUnion, progress chan ProgressCounter) {
	log.Printf("saving table unions")
	db, err := sql.Open("sqlite3", TableStatsDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (query_table text, candidate_table text, query_column int, candidate_column int, score real, measure text);`, TableStatsTable, TableStatsTable))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, query_column, candidate_column, score, measure) values(?, ?, ?, ?, ?, ?);`, TableStatsTable))
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for _, p := range union.alignment {
			for _, m := range p.measure {
				_, err = stmt.Exec(union.queryTable, union.candTable, p.queryColumn, p.candColumn, p.score, m)
				if err != nil {
					panic(err)
				}
			}
		}
		progress <- ProgressCounter{1}
		wg.Done()
	}()
	go func() {
		wg.Wait()
		db.Close()
	}()
}
