package opendata

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/RJMillerLab/table-union/embedding"
)

var (
	ByteOrder = binary.BigEndian
)

type attributeUnion struct {
	queryColumn int
	candColumn  int
	score       float64
	measure     []string
}

func ComputeUnionabilityScores(queryTable, candidateTable, domainDir string) []attributeUnion {
	seen := make(map[string]bool)
	union := make([]attributeUnion, 0)
	for _, qindex := range getTextDomains(queryTable) {
		for _, cindex := range getTextDomains(candidateTable) {
			if _, ok := seen[string(qindex)+" "+string(cindex)]; !ok {
				if _, ok := seen[string(cindex)+" "+string(qindex)]; !ok {
					seen[string(cindex)+" "+string(qindex)] = true
					seen[string(qindex)+" "+string(cindex)] = true
					score, measure := getAttUnionability(domainDir, queryTable, candidateTable, qindex, cindex)
					attunion := attributeUnion{
						queryColumn: qindex,
						candColumn:  cindex,
						score:       score,
						measure:     measure,
					}
					union = append(union, attunion)
				}
			}
		}

	}
	return union
}

func getAttUnionability(domainDir, queryTable, candidateTable string, queryIndex, candIndex int) (float64, []string) {
	var uScore float64
	uMeasure := make([]string, 0)
	uSet := setUnionability(domainDir, queryTable, candidateTable, queryIndex, candIndex)
	uScore = uSet
	uMeasure = append(uMeasure, "set")
	uNL := nlUnionability(domainDir, queryTable, candidateTable, queryIndex, candIndex)
	if uNL > uScore {
		uScore = uNL
		uMeasure = make([]string, 0)
		uMeasure = append(uMeasure, "nl")
	}
	if uNL == uScore {
		uMeasure = append(uMeasure, "nl")
	}
	uSemSet := semSetUnionability(domainDir, queryTable, candidateTable, queryIndex, candIndex)
	if uSemSet > uScore {
		uScore = uSemSet
		uMeasure = make([]string, 0)
		uMeasure = append(uMeasure, "semset")
	}
	if uSemSet == uScore {
		uMeasure = append(uMeasure, "semset")
	}
	return uScore, uMeasure
}

func semUnionability(domainDir, queryTable, candidateTable string, queryIndex, candIndex int) float64 {
	ontMinhashFilename := getOntMinhashFilename(candidateTable, domainDir, candIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return 0.0
	}
	coVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return 0.0
	}
	ontMinhashFilename = getOntMinhashFilename(queryTable, domainDir, queryIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return 0.0
	}
	qoVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return 0.0
	}
	ontJaccard := estimateJaccard(coVec, qoVec)
	_, nA := getOntDomainCardinality(candidateTable, domainDir, candIndex)
	_, nB := getOntDomainCardinality(queryTable, domainDir, queryIndex)
	ontProb := sameDomainProb(ontJaccard, nA, nB)
	return ontProb
}

func semSetUnionability(domainDir, queryTable, candidateTable string, queryIndex, candIndex int) float64 {
	minhashFilename := getUnannotatedMinhashFilename(candidateTable, domainDir, candIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		return 0.0
	}
	cuaVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		return 0.0
	}
	minhashFilename = getUnannotatedMinhashFilename(queryTable, domainDir, queryIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		return 0.0
	}
	quaVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		return 0.0
	}
	jaccard := estimateJaccard(quaVec, cuaVec)
	// computing ontology jaccard
	ontMinhashFilename := getOntMinhashFilename(candidateTable, domainDir, candIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return 0.0
	}
	coVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return 0.0
	}
	ontMinhashFilename = getOntMinhashFilename(queryTable, domainDir, queryIndex)
	if _, err := os.Stat(ontMinhashFilename); os.IsNotExist(err) {
		return 0.0
	}
	qoVec, err := ReadMinhashSignature(ontMinhashFilename, numHash)
	if err != nil {
		return 0.0
	}
	ontJaccard := estimateJaccard(coVec, qoVec)
	noA, nA := getOntDomainCardinality(candidateTable, domainDir, candIndex)
	noB, nB := getOntDomainCardinality(queryTable, domainDir, queryIndex)
	//	coverage := float64(queryCard-noOntQueryCard) / float64(queryCard)
	noOntProb := sameDomainProb(jaccard, noA, noB)
	ontProb := sameDomainProb(ontJaccard, nA, nB)
	return noOntProb + ontProb - ontProb*noOntProb
}

func nlUnionability(domainDir, queryTable, candidateTable string, queryIndex, candIndex int) float64 {
	meanFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-mean", candidateTable, candIndex))
	if _, err := os.Stat(meanFilename); os.IsNotExist(err) {
		log.Printf("Mean embedding file %s does not exist.", meanFilename)
		panic(err)
	}
	cMean, err := embedding.ReadVecFromDisk(meanFilename, ByteOrder)
	if err != nil {
		log.Printf("Error in reading %s from disk.", meanFilename)
		panic(err)
	}
	meanFilename = filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-mean", queryTable, queryIndex))
	if _, err := os.Stat(meanFilename); os.IsNotExist(err) {
		log.Printf("Mean embedding file %s does not exist.", meanFilename)
		panic(err)
	}
	qMean, err := embedding.ReadVecFromDisk(meanFilename, ByteOrder)
	if err != nil {
		log.Printf("Error in reading %s from disk.", meanFilename)
		panic(err)
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
	// inserting the pair into its corresponding priority queue
	//ht2, f := getT2Statistics(mean, queryMean, covar, queryCovar, card, queryCardinality)
	return cosine
}

func setUnionability(domainDir, queryTable, candidateTable string, queryIndex, candIndex int) float64 {
	// getting the embedding of the candidate column
	minhashFilename := getMinhashFilename(candidateTable, domainDir, candIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("Embedding file %s does not exist.", minhashFilename)
		panic(err)
	}
	cVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", minhashFilename)
		panic(err)
	}
	minhashFilename = getMinhashFilename(queryTable, domainDir, queryIndex)
	if _, err := os.Stat(minhashFilename); os.IsNotExist(err) {
		log.Printf("Embedding file %s does not exist.", minhashFilename)
		panic(err)
	}
	qVec, err := ReadMinhashSignature(minhashFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", minhashFilename)
		panic(err)
	}
	// inserting the pair into its corresponding priority queue
	jaccard := estimateJaccard(cVec, qVec)
	nB := getDomainCardinality(candidateTable, domainDir, candIndex)
	nA := getDomainCardinality(queryTable, domainDir, queryIndex)
	uSet := sameDomainProb(jaccard, nA, nB)
	return uSet
}

func getDomainCardinality(tableID, domainDir string, index int) int {
	cardpath := path.Join(domainDir, tableID)
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

func getMinhashFilename(tableID, domainDir string, index int) string {
	fullpath := path.Join(domainDir, tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "minhash"))
	return fullpath
}

func getDomainSize(tableID, domainDir string, index int) int {
	cardpath := path.Join(domainDir, tableID)
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

func getUnannotatedMinhashFilename(tableID, domainDir string, index int) string {
	fullpath := path.Join(domainDir, tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "noann-minhash"))
	return fullpath
}

func getOntMinhashFilename(tableID, domainDir string, index int) string {
	fullpath := path.Join(domainDir, tableID)
	fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "ont-minhash-l1"))
	//fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", index, "ont-minhash-l2"))
	return fullpath
}

func getOntDomainCardinality(tableID, domainDir string, index int) (int, int) {
	cardpath := path.Join(domainDir, tableID)
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
	ontCardpath := path.Join(domainDir, tableID)
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
