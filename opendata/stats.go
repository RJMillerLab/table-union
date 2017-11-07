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

type CDF struct {
	Histogram []Bin
	Width     float64
}

type tableCUnion struct {
	queryTable     string
	candidateTable string
	cScores        map[int]float64
	diffScores     map[int]float64
}

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

type Bin struct {
	LowerBound        float64
	UpperBound        float64
	Count             int
	AccumulativeCount int
	Percentile        float64
	Total             int
}

func ComputeAllAttUnionabilityScores(queryTable, candidateTable string) []AttributeUnion {
	union := make([]AttributeUnion, 0)
	queryTextDomains := getTextDomains(queryTable)
	candTextDomains := getTextDomains(candidateTable)
	for _, qindex := range queryTextDomains {
		for _, cindex := range candTextDomains {
			uSet, uSem, uSemSet, uNL := getAllAttUnionability(queryTable, candidateTable, qindex, cindex)
			if uSet != -1.0 {
				attunion := AttributeUnion{
					queryTable:  queryTable,
					candTable:   candidateTable,
					queryColumn: qindex,
					candColumn:  cindex,
					score:       uSet,
					measure:     []string{"set"},
				}
				union = append(union, attunion)
			}
			if uSem != -1.0 {
				attunion := AttributeUnion{
					queryTable:  queryTable,
					candTable:   candidateTable,
					queryColumn: qindex,
					candColumn:  cindex,
					score:       uSem,
					measure:     []string{"sem"},
				}
				union = append(union, attunion)
			}
			if uSemSet != -1.0 {
				attunion := AttributeUnion{
					queryTable:  queryTable,
					candTable:   candidateTable,
					queryColumn: qindex,
					candColumn:  cindex,
					score:       uSemSet,
					measure:     []string{"semset"},
				}
				union = append(union, attunion)
			}
			if uNL != -1.0 {
				attunion := AttributeUnion{
					queryTable:  queryTable,
					candTable:   candidateTable,
					queryColumn: qindex,
					candColumn:  cindex,
					score:       uNL,
					measure:     []string{"nl"},
				}
				union = append(union, attunion)
			}
		}

	}
	return union
}

func ComputeAttUnionabilityScores(queryTable, candidateTable string) ([]AttributeUnion, int, int) {
	union := make([]AttributeUnion, 0)
	queryTextDomains := getTextDomains(queryTable)
	candTextDomains := getTextDomains(candidateTable)
	queryColNum := len(queryTextDomains)
	candColNum := len(candTextDomains)
	for _, qindex := range queryTextDomains {
		for _, cindex := range candTextDomains {
			score, measure := GetAttUnionability(queryTable, candidateTable, qindex, cindex)
			if score == -1.0 {
				continue
			}
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

func getAllAttUnionability(queryTable, candidateTable string, queryIndex, candIndex int) (float64, float64, float64, float64) {
	uSet := setUnionability(queryTable, candidateTable, queryIndex, candIndex)
	uNL := nlUnionability(queryTable, candidateTable, queryIndex, candIndex)
	uSem, uSemSet := semSetUnionability(queryTable, candidateTable, queryIndex, candIndex)
	return uSet, uSem, uSemSet, uNL
}

func GetAttUnionabilityPercentile(queryTable, candidateTable string, queryIndex, candIndex int, setCDF, semCDF, semsetCDF, nlCDF CDF) (float64, []string) {
	var uScore float64
	uMeasure := make([]string, 0)
	uSet := getPercentile(setCDF, setUnionability(queryTable, candidateTable, queryIndex, candIndex))
	uScore = uSet
	uMeasure = append(uMeasure, "set")
	uNL := getPercentile(nlCDF, nlUnionability(queryTable, candidateTable, queryIndex, candIndex))
	if uNL > uScore {
		uScore = uNL
		uMeasure = make([]string, 0)
		uMeasure = append(uMeasure, "nl")
	} else if uNL == uScore {
		uMeasure = append(uMeasure, "nl")
	}
	uSem, uSemSet := semSetUnionability(queryTable, candidateTable, queryIndex, candIndex)
	uSem = getPercentile(semCDF, uSem)
	uSemSet = getPercentile(semsetCDF, uSemSet)
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

func GetAttUnionability(queryTable, candidateTable string, queryIndex, candIndex int) (float64, []string) {
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

func semSetUnionability(queryTable, candidateTable string, queryIndex, candIndex int) (float64, float64) {
	minhashFilename := getUnannotatedMinhashFilename(candidateTable, candIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("file %s does not exist.", minhashFilename)
		return -1.0, -1.0
	}
	cuaVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		return -1.0, -1.0
	}
	minhashFilename = getUnannotatedMinhashFilename(queryTable, queryIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("file %s does not exist.", minhashFilename)
		return -1.0, -1.0
	}
	quaVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		return -1.0, -1.0
	}
	jaccard := estimateJaccard(quaVec, cuaVec)
	// computing ontology jaccard
	ontMinhashFilename := getOntMinhashFilename(candidateTable, candIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		log.Printf("file %s does not exist.", ontMinhashFilename)
		return -1.0, -1.0
	}
	coVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		log.Printf("file %s does not exist.", ontMinhashFilename)
		return -1.0, -1.0
	}
	ontMinhashFilename = getOntMinhashFilename(queryTable, queryIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return -1.0, -1.0
	}
	qoVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return -1.0, -1.0
	}
	ontJaccard := estimateJaccard(coVec, qoVec)
	noA, nA := getOntDomainCardinality(candidateTable, candIndex)
	noB, nB := getOntDomainCardinality(queryTable, queryIndex)
	if noA == -1.0 || nA == -1.0 || noB == -1.0 || nB == -1.0 {
		return -1.0, -1.0
	}
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
	if nB == -1.0 || nA == -1.0 {
		return -1.0
	}
	uSet := sameDomainProb(jaccard, nA, nB)
	return uSet
}

func getDomainCardinality(tableID string, index int) int {
	cardpath := path.Join(OutputDir, "domains", tableID)
	cardpath = path.Join(cardpath, fmt.Sprintf("%d.%s", index, "card"))
	f, err := os.Open(cardpath)
	defer f.Close()
	if err != nil {
		return -1.0
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
	fullpath := path.Join(OutputDir, "domains", tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "ont-minhash-l1"))
	//fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "ont-minhash-l2"))
	return fullpath
}

func getOntDomainCardinality(tableID string, index int) (int, int) {
	cardpath := path.Join(OutputDir, "domains", tableID)
	cardpath = path.Join(cardpath, fmt.Sprintf("%d.%s", index, "ont-noann-card"))
	f, err := os.Open(cardpath)
	defer f.Close()
	if err != nil {
		return -1.0, -1.0
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
		return -1.0, -1.0
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

func DoSaveAttScores(allScores chan []AttributeUnion, progress chan ProgressCounter) {
	db, err := sql.Open("sqlite3", AttStatsDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (query_table text,query_column int, candidate_table text,candidate_column int, score real, measure text);`, AllAttStatsTable, AllAttStatsTable))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, query_column, candidate_table, candidate_column, score, measure) values(?, ?, ?, ?, ?, ?);`, AllAttStatsTable))
	if err != nil {
		panic(err)
	}
	for scores := range allScores {
		for _, score := range scores {
			for _, m := range score.measure {
				_, err = stmt.Exec(score.queryTable, score.queryColumn, score.candTable, score.candColumn, score.score, m)
				if err != nil {
					panic(err)
				}
				progress <- ProgressCounter{1}
			}
		}
	}
	db.Close()
}

func DoSaveTableScores(unions chan TableUnion, progress chan ProgressCounter) {
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
	//wg := &sync.WaitGroup{}
	//wg.Add(1)
	//go func() {
	for union := range unions {
		for _, p := range union.alignment {
			for _, m := range p.measure {
				_, err = stmt.Exec(union.queryTable, union.candTable, p.queryColumn, p.candColumn, p.score, m)
				if err != nil {
					panic(err)
				}
			}
		}
		progress <- ProgressCounter{1}
	}
	db.Close()
	//	wg.Done()
	//}()
	//go func() {
	//	wg.Wait()
	//	db.Close()
	//}()
}

func ComputeTableUnionabilityVariousC() {
	tableUnions := make(chan tableCUnion)
	db, err := sql.Open("sqlite3", AttStatsDB)
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT query_table, candidate_table FROM %s LIMIT 10000;`, AttStatsTable))
	if err != nil {
		panic(err)
	}
	//
	tablePairs := make([]string, 0)
	for rows.Next() {
		var queryTable string
		var candidateTable string
		err := rows.Scan(&queryTable, &candidateTable)
		if err != nil {
			panic(err)
		}
		tablePairs = append(tablePairs, queryTable+" "+candidateTable)
	}
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for _, pair := range tablePairs {
			rows, err = db.Query(fmt.Sprintf(`SELECT DISTINCT query_column, candidate_column, MAX(score) as attScore FROM %s WHERE query_table='%s' AND candidate_table='%s' GROUP BY query_column, candidate_column ORDER BY attScore DESC;`, AllAttStatsTable, strings.Split(pair, " ")[0], strings.Split(pair, " ")[1]))
			if err != nil {
				panic(err)
			}
			cUnionabilityScores := make(map[int]float64)
			unionabilityDiffs := make(map[int]float64)
			c := 1
			for rows.Next() {
				var score float64
				var queryTable string
				var candidateTable string
				err := rows.Scan(&queryTable, &candidateTable, &score)
				if err != nil {
					panic(err)
				}
				if c == 1 {
					cUnionabilityScores[c] = score
					unionabilityDiffs[c] = 1.0 - score
				} else {
					cUnionabilityScores[c] = cUnionabilityScores[c-1] * score
					unionabilityDiffs[c] = cUnionabilityScores[c-1] - cUnionabilityScores[c]
				}
				c += 1
			}
			cus := tableCUnion{
				queryTable:     strings.Split(pair, " ")[0],
				candidateTable: strings.Split(pair, " ")[1],
				cScores:        cUnionabilityScores,
				diffScores:     unionabilityDiffs,
			}
			tableUnions <- cus
		}
		db.Close()
		close(tableUnions)
		wg.Done()
	}()
	go func() {
		saveTableUnionabilityVariousC(tableUnions)
		wg.Done()
	}()
	wg.Wait()
}

func saveTableUnionabilityVariousC(unions chan tableCUnion) {
	db, err := sql.Open("sqlite3", TableStatsDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (query_table text, candidate_table text, c int, score real, difference real);`, CTableStatsTable, CTableStatsTable))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, c, score, difference) values(?, ?, ?, ?, ?);`, CTableStatsTable))
	if err != nil {
		panic(err)
	}
	count := 0
	for cus := range unions {
		queryTable := cus.queryTable
		candidateTable := cus.candidateTable
		for c, score := range cus.cScores {
			_, err = stmt.Exec(queryTable, candidateTable, c, score, cus.diffScores[c])
			if err != nil {
				panic(err)
			}
		}
		count += 1
		if count%50 == 0 {
			log.Printf("Saved %d table unionability.", count)
		}
	}
	db.Close()
}

func ComputeTableUnionabilityCDF(numBins int) {
	cCDFs := computeCUnionabilityCDFEquiWidth(numBins, TableStatsDB, CTableStatsTable)
	saveMultCDF(cCDFs, TableStatsDB, TableCDFTable)
}

func ComputeAttUnionabilityCDF(numBins int) {
	cdf := computeCDFEquiWidth(numBins, AttStatsDB, AttStatsTable)
	saveCDF(cdf, AttStatsDB, AttCDFTable)
}

func ComputeAllAttUnionabilityCDF(numBins int) {
	setCdf := computeAttCDFEquiWidth(numBins, AttStatsDB, AttStatsTable, "set")
	saveCDF(setCdf, AttStatsDB, SetCDFTable)
	semCdf := computeAttCDFEquiWidth(numBins, AttStatsDB, AttStatsTable, "sem")
	saveCDF(semCdf, AttStatsDB, SemCDFTable)
	semSetCdf := computeAttCDFEquiWidth(numBins, AttStatsDB, AttStatsTable, "semset")
	saveCDF(semSetCdf, AttStatsDB, SemSetCDFTable)
	nlCdf := computeAttCDFEquiWidth(numBins, AttStatsDB, AttStatsTable, "nl")
	saveCDF(nlCdf, AttStatsDB, NlCDFTable)
}

func computeCUnionabilityCDFEquiWidth(numBins int, dbName, tableName string) map[int][]Bin {
	var total int
	var maxC int
	cCDFs := make(map[int][]Bin)
	binWidth := 1.0 / float64(numBins)
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT c FROM %s;`, tableName))
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		err = rows.Scan(&maxC)
		if err != nil {
			panic(err)
		}
	}
	for c := 1; c <= maxC; c++ {
		cdf := make([]Bin, 0)
		rows, err := db.Query(fmt.Sprintf(`SELECT count(*) as count FROM %s WHERE c=%d;`, tableName, c))
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			err = rows.Scan(&total)
			if err != nil {
				panic(err)
			}
		}
		rows, err = db.Query(fmt.Sprintf(`SELECT score, count(*) as count FROM %s WHERE c=%d GROUP BY score ORDER BY score ASC;`, tableName, c))
		if err != nil {
			panic(err)
		}
		var i int
		var j int
		var binSize int
		b := Bin{
			LowerBound:        0.0,
			UpperBound:        binWidth,
			Count:             binSize,
			AccumulativeCount: 0,
			Percentile:        0.0,
			Total:             total,
		}
		cdf = append(cdf, b)
		for rows.Next() {
			var score float64
			var count int
			err = rows.Scan(&score, &count)
			if err != nil {
				panic(err)
			}
			if score >= float64(i+1)*binWidth && i < (numBins-1) {
				i += 1
				b := Bin{
					LowerBound:        float64(i) * binWidth,
					UpperBound:        float64(i+1) * binWidth,
					AccumulativeCount: j,
					Percentile:        float64(j) / float64(total),
					Total:             total,
				}
				cdf[len(cdf)-1].Count = binSize
				cdf = append(cdf, b)
				binSize = 0
			}
			binSize += count
			j += count
		}
		cdf[len(cdf)-1].Count = binSize
		if len(cdf) < numBins {
			for i := len(cdf); i < numBins; i += 1 {
				b := Bin{
					LowerBound:        float64(i) * binWidth,
					UpperBound:        float64(i+1) * binWidth,
					Count:             0,
					AccumulativeCount: cdf[len(cdf)-1].AccumulativeCount,
					Percentile:        cdf[len(cdf)-1].Percentile,
					Total:             cdf[len(cdf)-1].Total,
				}
				cdf = append(cdf, b)
			}
		}
		cCDFs[c] = cdf
		log.Printf("Number of bins for c = %d is %d.", c, len(cdf))
	}
	db.Close()
	return cCDFs
}

func computeAttCDFEquiWidth(numBins int, dbName, tableName, measure string) []Bin {
	cdf := make([]Bin, 0)
	var total int
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT count(*) as count FROM %s WHERE measure='%s';`, tableName, measure))
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		err = rows.Scan(&total)
		if err != nil {
			panic(err)
		}
	}
	binWidth := 1.0 / float64(numBins)
	rows, err = db.Query(fmt.Sprintf(`SELECT score, count(*) as count FROM %s WHERE measure='%s' GROUP BY score ORDER BY score ASC;`, tableName, measure))
	if err != nil {
		panic(err)
	}
	var i int
	var j int
	var binSize int
	b := Bin{
		LowerBound:        0.0,
		UpperBound:        binWidth,
		Count:             binSize,
		AccumulativeCount: 0,
		Percentile:        0.0,
		Total:             total,
	}
	cdf = append(cdf, b)
	for rows.Next() {
		var score float64
		var count int
		err = rows.Scan(&score, &count)
		if err != nil {
			panic(err)
		}
		if score >= float64(i+1)*binWidth && i < (numBins-1) {
			i += 1
			b := Bin{
				LowerBound:        float64(i) * binWidth,
				UpperBound:        float64(i+1) * binWidth,
				AccumulativeCount: j,
				Percentile:        float64(j) / float64(total),
				Total:             total,
			}
			cdf[len(cdf)-1].Count = binSize
			cdf = append(cdf, b)
			binSize = 0
		}
		binSize += count
		j += count
	}
	cdf[len(cdf)-1].Count = binSize
	if len(cdf) < numBins {
		for i := len(cdf); i < numBins; i += 1 {
			b := Bin{
				LowerBound:        float64(i) * binWidth,
				UpperBound:        float64(i+1) * binWidth,
				Count:             0,
				AccumulativeCount: cdf[len(cdf)-1].AccumulativeCount,
				Percentile:        cdf[len(cdf)-1].Percentile,
				Total:             cdf[len(cdf)-1].Total,
			}
			cdf = append(cdf, b)
		}
	}
	log.Printf("Number of bins %d.", len(cdf))
	db.Close()
	return cdf
}

func computeCDFEquiWidth(numBins int, dbName, tableName string) []Bin {
	cdf := make([]Bin, 0)
	var total int
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT count(*) as count FROM %s;`, tableName))
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		err = rows.Scan(&total)
		if err != nil {
			panic(err)
		}
	}
	binWidth := 1.0 / float64(numBins)
	rows, err = db.Query(fmt.Sprintf(`SELECT score, count(*) as count FROM %s GROUP BY score ORDER BY score ASC;`, tableName))
	if err != nil {
		panic(err)
	}
	var i int
	var j int
	var binSize int
	b := Bin{
		LowerBound:        0.0,
		UpperBound:        binWidth,
		Count:             binSize,
		AccumulativeCount: 0,
		Percentile:        0.0,
		Total:             total,
	}
	cdf = append(cdf, b)
	for rows.Next() {
		var score float64
		var count int
		err = rows.Scan(&score, &count)
		if err != nil {
			panic(err)
		}
		if score >= float64(i+1)*binWidth && i < (numBins-1) {
			i += 1
			b := Bin{
				LowerBound:        float64(i) * binWidth,
				UpperBound:        float64(i+1) * binWidth,
				AccumulativeCount: j,
				Percentile:        float64(j) / float64(total),
				Total:             total,
			}
			cdf[len(cdf)-1].Count = binSize
			cdf = append(cdf, b)
			binSize = 0
		}
		binSize += count
		j += count
	}
	cdf[len(cdf)-1].Count = binSize
	if len(cdf) < numBins {
		for i := len(cdf); i < numBins; i += 1 {
			b := Bin{
				LowerBound:        float64(i) * binWidth,
				UpperBound:        float64(i+1) * binWidth,
				Count:             0,
				AccumulativeCount: cdf[len(cdf)-1].AccumulativeCount,
				Percentile:        cdf[len(cdf)-1].Percentile,
				Total:             cdf[len(cdf)-1].Total,
			}
			cdf = append(cdf, b)
		}
	}
	log.Printf("Number of bins %d.", len(cdf))
	db.Close()
	return cdf
}

func saveMultCDF(cbins map[int][]Bin, dbName, tableName string) {
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (bin_num int, lower_bound real, upper_bound real, b_count int, accumulative_count int, percentile real, total int, c int);`, tableName, tableName))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(bin_num, lower_bound, upper_bound, b_count, accumulative_count, percentile, total, c) values(?, ?, ?, ?, ?, ?, ?, ?);`, tableName))
	if err != nil {
		panic(err)
	}
	for c, bins := range cbins {
		for i, b := range bins {
			_, err = stmt.Exec(i, b.LowerBound, b.UpperBound, b.Count, b.AccumulativeCount, b.Percentile, b.Total, c)
			if err != nil {
				panic(err)
			}
		}
	}
	db.Close()
}

func saveCDF(bins []Bin, dbName, tableName string) {
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (bin_num int, lower_bound real, upper_bound real, b_count int, accumulative_count int, percentile real, total int);`, tableName, tableName))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(bin_num, lower_bound, upper_bound, b_count, accumulative_count, percentile, total) values(?, ?, ?, ?, ?, ?, ?);`, tableName))
	if err != nil {
		panic(err)
	}
	for i, b := range bins {
		_, err = stmt.Exec(i, b.LowerBound, b.UpperBound, b.Count, b.AccumulativeCount, b.Percentile, b.Total)
		if err != nil {
			panic(err)
		}
	}
	db.Close()
}

func LoadCDF() (CDF, CDF, CDF, CDF, map[int]CDF) {
	setCDF := readCDFFromDB(AttStatsDB, SetCDFTable)
	semCDF := readCDFFromDB(AttStatsDB, SemCDFTable)
	semsetCDF := readCDFFromDB(AttStatsDB, SemSetCDFTable)
	nlCDF := readCDFFromDB(AttStatsDB, NlCDFTable)
	tableCDF := readMultCDFFromDB(TableStatsDB, TableCDFTable)
	return setCDF, semCDF, semsetCDF, nlCDF, tableCDF
}

func readMultCDFFromDB(dbName, tableName string) map[int]CDF {
	cdfs := make(map[int]CDF)
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT c FROM %s;`, tableName))
	if err != nil {
		panic(err)
	}
	var maxC int
	for rows.Next() {
		err := rows.Scan(&maxC)
		if err != nil {
			panic(err)
		}
	}
	for c := 1; c <= maxC; c++ {
		rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT bin_num, lower_bound, upper_bound, b_count, accumulative_count, percentile, total FROM %s WHERE c = %d;`, tableName, c))
		if err != nil {
			panic(err)
		}
		//
		hist := make([]Bin, 0)
		for rows.Next() {
			var binNum int
			var lowerBound float64
			var upperBound float64
			var count int
			var accumulativeCount int
			var percentile float64
			var total int
			err := rows.Scan(&binNum, &lowerBound, &upperBound, &count, &accumulativeCount, &percentile, &total)
			if err != nil {
				panic(err)
			}
			bin := Bin{
				LowerBound:        lowerBound,
				UpperBound:        upperBound,
				Count:             count,
				AccumulativeCount: accumulativeCount,
				Percentile:        percentile,
				Total:             total,
			}
			hist = append(hist, bin)
		}
		cdf := CDF{
			Histogram: hist,
			Width:     1.0 / float64(len(hist)),
		}
		cdfs[c] = cdf
	}
	db.Close()
	return cdfs
}

func readCDFFromDB(dbName, tableName string) CDF {
	hist := make([]Bin, 0)
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT bin_num, lower_bound, upper_bound, b_count, accumulative_count, percentile, total FROM %s;`, tableName))
	if err != nil {
		panic(err)
	}
	//
	for rows.Next() {
		var binNum int
		var lowerBound float64
		var upperBound float64
		var count int
		var accumulativeCount int
		var percentile float64
		var total int
		err := rows.Scan(&binNum, &lowerBound, &upperBound, &count, &accumulativeCount, &percentile, &total)
		if err != nil {
			panic(err)
		}
		bin := Bin{
			LowerBound:        lowerBound,
			UpperBound:        upperBound,
			Count:             count,
			AccumulativeCount: accumulativeCount,
			Percentile:        percentile,
			Total:             total,
		}
		hist = append(hist, bin)
	}
	db.Close()
	cdf := CDF{
		Histogram: hist,
		Width:     1.0 / float64(len(hist)),
	}
	return cdf
}

// getPercentile returns the percentile of a score as a number between 0 and 1
func getPercentile(cdf CDF, score float64) float64 {
	if score > 1.0 || score < 0.0 {
		return 0.0
	}
	binWidth := cdf.Width
	id := 0
	if score != 0.0 {
		//id = int(math.Min(float64(len(cdf.Histogram)-1), math.Floor(math.Exp(math.Log(score)-math.Log(binWidth)))))
		id = int(math.Min(float64(len(cdf.Histogram)-1), math.Floor(score/binWidth)))
	}
	bin := cdf.Histogram[id]
	percentile := bin.Percentile
	//detail := (float64(bin.Count) * math.Exp(math.Log(score-bin.LowerBound)-math.Log(bin.UpperBound-bin.LowerBound))) / float64(bin.Total)
	detail := (float64(bin.Count) * ((score - bin.LowerBound) / binWidth)) / float64(bin.Total)
	if detail > 1.0 || percentile > 1.0 {
		log.Printf("error in percentile.")
	}
	return percentile + detail
}
