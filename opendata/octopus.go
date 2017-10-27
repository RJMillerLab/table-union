package opendata

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ekzhu/datatable"
)

type Slice struct {
	sort.Float64Slice
	idx []int
}

func (s Slice) Swap(i, j int) {
	s.Float64Slice.Swap(i, j)
	s.idx[i], s.idx[j] = s.idx[j], s.idx[i]
}

func NewSlice(n ...float64) *Slice {
	s := &Slice{Float64Slice: sort.Float64Slice(n), idx: make([]int, len(n))}
	for i := range s.idx {
		s.idx[i] = i
	}
	return s
}

type OctopusScore struct {
	query     string
	candidate string
	score     float64
}

func ComputeIDF(filenames <-chan string) map[string]float64 {
	numDocuments := 0
	idf := make(map[string]float64)
	for filename := range filenames {
		numDocuments += 1
		filename := path.Join(OpendataDir, filename)
		tFile, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		table, err := datatable.FromCSV(csv.NewReader(tFile))
		for i := 0; i < table.NumRow(); i++ {
			row := table.GetRow(i)
			for j := 0; j < table.NumCol(); j++ {
				cell := strings.TrimSpace(row[j])
				if len(cell) != 0 {
					if _, ok := idf[cell]; ok {
						idf[cell] += 1
					} else {
						idf[cell] = 1
					}
				}
			}
		}
		tFile.Close()
	}
	// computing idf
	for t, df := range idf {
		idf[t] = 1 + math.Log(float64(numDocuments)/df)
	}
	return idf
}

func computeTableTFIDF(filename string, idf map[string]float64) (map[string]float64, float64) {
	tf := make(map[string]float64)
	filename = path.Join(OpendataDir, filename)
	tFile, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer tFile.Close()
	table, err := datatable.FromCSV(csv.NewReader(tFile))
	numTokens := 0
	for i := 0; i < table.NumRow(); i++ {
		row := table.GetRow(i)
		for j := 0; j < table.NumCol(); j++ {
			cell := strings.TrimSpace(row[j])
			if len(cell) != 0 {
				numTokens += 1
				if _, ok := tf[cell]; ok {
					tf[cell] += 1
				} else {
					tf[cell] = 1
				}
			}
		}
	}
	// compute tf-idf and L2 norm
	tfidf := make(map[string]float64)
	l2 := 0.0
	for t, f := range tf {
		tf[t] = float64(f) / float64(numTokens)
		tfidf[t] = tf[t] * idf[t]
		l2 += math.Pow(tf[t]*idf[t], 2.0)
	}
	l2 = math.Sqrt(l2)
	return tfidf, l2
}

func ComputeColumnTextClusterScore(t1name, t2name string, tfidfs1, tfidfs2 []map[string]float64, l2s1, l2s2 []float64) OctopusScore {
	seen := make(map[string]bool)
	cdists := make([]float64, 0)
	colpairs := make([]string, 0)
	for i, _ := range tfidfs1 {
		for j, _ := range tfidfs2 {
			if _, ok := seen[string(i)+" "+string(j)]; !ok {
				if _, ok := seen[string(j)+" "+string(i)]; !ok {
					colpairs = append(colpairs, string(i)+" "+string(j))
					cdists = append(cdists, 1.0-computeCosine(tfidfs1[i], tfidfs2[j], l2s1[i], l2s2[j]))
				}
			}
		}
	}
	s := NewSlice(cdists...)
	sort.Sort(s)
	m := int(math.Min(float64(len(tfidfs1)), float64(len(tfidfs2))))
	var matchNum int
	covered := make(map[int]bool)
	var score float64
	for jx, ix := range s.idx {
		i, _ := strconv.Atoi(strings.Split(colpairs[ix], " ")[0])
		j, _ := strconv.Atoi(strings.Split(colpairs[ix], " ")[1])
		if _, ok := covered[i]; !ok {
			if _, ok := covered[j]; !ok {
				score += 1.0 - s.Float64Slice[jx]
				matchNum += 1
			}
		}

		if matchNum == m {
			break
		}
	}

	sp := OctopusScore{
		query:     t1name,
		candidate: t2name,
		score:     score,
	}
	return sp
}

func ComputeSizeClusterScore(t1name, t2name string, lens1, lens2 []float64) OctopusScore {
	inx := 0
	jnx := 0
	sum := 0.0
	anum := 1
	m := int(math.Min(float64(len(lens1)), float64(len(lens2))))
	for i := 0; i < m; i++ {
		if anum == m {
			break
		}
		a := math.Abs(lens1[len(lens1)-1-inx] - lens2[jnx])
		b := math.Abs(lens2[len(lens2)-1-jnx] - lens1[inx])
		if a < b {
			sum += b
			jnx += 1
			anum += 1
		} else {
			sum += a
			inx += 1
			anum += 1
		}
	}
	sp := OctopusScore{
		query:     t1name,
		candidate: t2name,
		score:     sum,
	}
	return sp
}

func ComputeTextClusterScore(t1name, t2name string, idf map[string]float64) OctopusScore {
	t1Vec := make(map[string]float64)
	t2Vec := make(map[string]float64)
	var t1L2 float64
	var t2L2 float64
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		t1Vec, t1L2 = computeTableTFIDF(t1name, idf)
		wg.Done()
	}()
	go func() {
		t2Vec, t2L2 = computeTableTFIDF(t2name, idf)
		wg.Done()
	}()
	wg.Wait()
	cosine := computeCosine(t1Vec, t2Vec, t1L2, t2L2)
	sp := OctopusScore{
		query:     t1name,
		candidate: t2name,
		score:     cosine,
	}
	return sp
}

func computeCosine(v1, v2 map[string]float64, l21, l22 float64) float64 {
	var dotProduct float64
	for t, s := range v1 {
		if _, ok := v2[t]; ok {
			dotProduct += s * v2[t]
		}
	}
	cosine := dotProduct / float64(l21*l22)
	return cosine
}

func computeColumnTFIDF(tablename string, index int, idf map[string]float64) (map[string]float64, float64) {
	tf := make(map[string]float64)
	filepath := path.Join(OutputDir, "domains", tablename, fmt.Sprintf("%d.values", index))
	f, err := os.Open(filepath)
	defer f.Close()
	if err != nil {
		panic(err)
	}
	numTokens := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		cell := strings.TrimSpace(scanner.Text())
		if len(cell) != 0 {
			numTokens += 1
			if _, ok := tf[cell]; ok {
				tf[cell] += 1
			} else {
				tf[cell] = 1
			}
		}
	}
	// compute tf-idf and L2 norm
	tfidf := make(map[string]float64)
	l2 := 0.0
	for t, f := range tf {
		tf[t] = float64(f) / float64(numTokens)
		tfidf[t] = tf[t] * idf[t]
		l2 += math.Pow(tf[t]*idf[t], 2.0)
	}
	l2 = math.Sqrt(l2)
	return tfidf, l2
}

func DoSaveOctopusScores(scores <-chan OctopusScore) <-chan ProgressCounter {
	log.Printf("saving octopus scores")
	db, err := sql.Open("sqlite3", OctopusDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (query_table text, candidate_table text, score real);`, OctopusTable, OctopusTable))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, score) values(?, ?, ?);`, OctopusTable))
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	progress := make(chan ProgressCounter)
	go func() {
		for score := range scores {
			//log.Printf("saving the score of %s and %s: %f", score.query, score.candidate, score.score)
			_, err = stmt.Exec(score.query, score.candidate, score.score)
			if err != nil {
				panic(err)
			}
			progress <- ProgressCounter{1}
		}
		wg.Done()
	}()
	go func() {
		wg.Wait()
		db.Close()
		close(progress)
	}()
	return progress
}

func GetTableColumnsTFIDF(tablename string, idf map[string]float64) ([]map[string]float64, []float64) {
	coltfidfs := make([]map[string]float64, 0)
	l2s := make([]float64, 0)
	for _, index := range getNonNumericDomains(tablename) {
		tfidf, l2 := computeColumnTFIDF(tablename, index, idf)
		coltfidfs = append(coltfidfs, tfidf)
		l2s = append(l2s, l2)
	}
	return coltfidfs, l2s
}

func GetTableColumnMeanLength(tablename string) []float64 {
	colens := make([]float64, 0)
	for _, index := range getNonNumericDomains(tablename) {
		colens = append(colens, getMeanLength(tablename, index))
	}
	return colens
}

func getMeanLength(tablename string, index int) float64 {
	filepath := path.Join(OutputDir, "domains", tablename, fmt.Sprintf("%d.values", index))
	f, err := os.Open(filepath)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	var card int
	scanner := bufio.NewScanner(f)
	lengthSum := 0.0
	for scanner.Scan() {
		val := scanner.Text()
		if len(val) > 0 {
			card += 1
			lengthSum += float64(len(val))
		}
	}
	return lengthSum / float64(card)
}
