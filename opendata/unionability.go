package opendata

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/RJMillerLab/table-union/embedding"
)

var (
	lookup        map[string]map[string]bool
	counts        map[string]int
	notLetter     = regexp.MustCompile(`[^a-z]+`)
	space         = []byte(" ")
	minK          = 1
	maxK          = 10
	scoreFilename = "/home/fnargesian/TABLE_UNION_OUTPUT/scores/col_unionability"
)

type tableUnionability struct {
	table1  string
	table2  string
	measure string
	score   float64
	k       int
}

type columnUnionability struct {
	Table1      string
	Column1     int
	Table2      string
	Column2     int
	Sarma       float64
	Jaccard     float64
	Containment float64
	Cosine      float64
}

type tablePair struct {
	queryTable string
	codTable   string
}

func Init() {
	lookup = LoadEntityWords()
	counts = LoadEntityWordCount()
}

func MkTablePairs(queries, files []string) <-chan *tablePair {
	// randomize the list of pairs
	out := make(chan *tablePair)
	pairs := make([]*tablePair, len(queries)*len(files))
	go func() {
		j := 0
		for _, q := range queries {
			for _, f := range files {
				tp := &tablePair{
					queryTable: q,
					codTable:   f,
				}
				pairs[j] = tp
				j += 1
			}

		}
		for _, i := range rand.Perm(len(pairs)) {
			out <- pairs[i]
		}
		close(out)
	}()
	return out
}

func DoComputeUnionability(queries, files []string, fanout int) <-chan *columnUnionability {
	compare := MkTablePairs(queries, files)
	out := make(chan *columnUnionability, 1000000)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	count := 0
	for i := 0; i < fanout; i++ {
		go func(id int) {
			for pair := range compare {
				count += 1
				for score := range compareTables(pair.codTable, pair.queryTable) {
					out <- score
				}
				if count%200 == 0 {
					log.Printf("Compared %d tables", count)
				}
			}
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		log.Printf("done comparing")
		close(out)
	}()
	return out
}

func ComputeAndSaveUnionability(queries, files []string, fanout int) <-chan ProgressCounter {
	compare := MkTablePairs(queries, files)
	progress := make(chan ProgressCounter)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	count := 0
	for i := 0; i < fanout; i++ {
		go func(id int) {
			fname := scoreFilename + "_" + strconv.Itoa(id) + ".csv"
			log.Printf("fname: %s", fname)
			f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
			for pair := range compare {
				count += 1
				for score := range compareTables(pair.codTable, pair.queryTable) {
					_, err := f.WriteString(score.Table1 + "," + strconv.Itoa(score.Column1) + "," + score.Table2 + "," + strconv.Itoa(score.Column2) + "," + strconv.FormatFloat(score.Sarma, 'f', 6, 64) + "," + strconv.FormatFloat(score.Cosine, 'f', 6, 64) + ", " + strconv.FormatFloat(score.Jaccard, 'f', 6, 64) + "," + strconv.FormatFloat(score.Containment, 'f', 6, 64) + "\n")
					if err != nil {
						panic(err)
					}
					progress <- ProgressCounter{1}
				}
				if count%200 == 0 {
					log.Printf("Compared %d tables", count)
				}
			}
			f.Close()
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		log.Printf("done comparing")
		close(progress)
	}()
	return progress
}

func compareTables(codFilename, queryFilename string) <-chan *columnUnionability {
	out := make(chan *columnUnionability)
	go func() {
		for _, col1 := range getTextDomains(queryFilename) {
			for _, col2 := range getTextDomains(codFilename) {
				sarma, jaccard, containment, cosine := unionability(queryFilename, col1, codFilename, col2)
				out <- &columnUnionability{
					Table1:      queryFilename,
					Column1:     col1,
					Table2:      codFilename,
					Column2:     col2,
					Sarma:       sarma,
					Jaccard:     jaccard,
					Containment: containment,
					Cosine:      cosine,
				}
			}
		}
		close(out)
	}()
	return out
}

func unionability(table1 string, column1 int, table2 string, column2 int) (float64, float64, float64, float64) {
	domain1Values := unique(getDomainWords(table1, column1))
	domain2Values := unique(getDomainWords(table2, column2))
	//difference, intersection := differenceAndIntersection(domain2Values, domain1Values)
	_, intersection := differenceAndIntersection(domain2Values, domain1Values)
	// computing scores
	//sarma := getSarma(domain1Values, difference)
	sarma := -2.0
	jaccard := getJaccard(domain1Values, domain1Values, intersection)
	containment := getContainment(domain1Values, domain1Values, intersection)
	cosine := getCosineEmbSum(table1, column1, table2, column2)
	return sarma, jaccard, containment, cosine
}

func getSarma(query, expansion []string) float64 {
	annotations1 := annotate(query)
	annotations2 := annotate(expansion)
	n1 := 1.0
	n2 := 1.0
	m1 := 1.0
	m2 := 0.0
	for a, f := range annotations1 {
		annotations1[a] = math.Pow(f, n1) / math.Pow(float64(len(query)), m1)
	}
	for a, f := range annotations2 {
		annotations2[a] = math.Pow(f, n2) / math.Pow(float64(len(expansion)), m2)
	}
	sarma := 0.0
	for c, s := range annotations1 {
		if t, ok := annotations2[c]; ok {
			sarma += s * t
		}
	}
	return sarma
}

func getJaccard(query, candidate, intersection []string) float64 {
	return float64(len(intersection)) / float64(len(query)+len(candidate)-len(intersection))
}

func getContainment(query, candidate, intersection []string) float64 {
	return float64(len(intersection)) / float64(len(query))
}

func DoAlign(queries, files []string, fanout int) <-chan *tableUnionability {
	compare := MkTablePairs(queries, files)
	out := make(chan *tableUnionability)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int) {
			for pair := range compare {
				for score := range alignTables(pair.codTable, pair.queryTable) {
					out <- score
				}
			}
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		log.Printf("done computing")
		close(out)
	}()
	return out
}

func alignTables(table1, table2 string) <-chan *tableUnionability {
	db, err := sql.Open("sqlite3", ColumnUnionabilityDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	rows, err := db.Query(fmt.Sprintf(`SELECT column1, column2, %s 
						FROM %s WHERE table1="%s" AND table2="%s" 
						AND %s > %s ORDER BY %s;`, Measure, ColumnUnionabilityTable, table1, table2, Measure, Threshold, Measure))
	if err != nil {
		panic(err)
	}
	out := make(chan *tableUnionability)
	go func(rows *sql.Rows) {
		// greedy alignment
		source := make(map[int]bool)
		dest := make(map[int]bool)
		var k int
		for rows.Next() {
			var column1 int
			var column2 int
			var score float64
			err := rows.Scan(&column1, &column2, &score)
			if err != nil {
				panic(err)
			}
			if _, ok := source[column1]; !ok {
				if _, ok := dest[column2]; !ok {
					k += 1
					source[column1] = true
					dest[column2] = true
					if k >= minK && k <= maxK {
						out <- &tableUnionability{
							table1:  table1,
							table2:  table2,
							measure: Measure,
							score:   score,
							k:       k,
						}

					}
				}
			}
		}
		close(out)
		rows.Close()
	}(rows)
	return out
}

func DoSaveKUnionabilityScores(scores <-chan *tableUnionability, fanout int) <-chan ProgressCounter {
	progress := make(chan ProgressCounter)
	wg := &sync.WaitGroup{}
	// output db
	db, err := sql.Open("sqlite3", DatasetUnionabilityDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(
		`drop table if exists %s; create table if not exists %s (
			table1 text,
			table2 text,
			measure text, 
			k int, 
			score real);`, DatasetUnionabilityTable, DatasetUnionabilityTable))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db.Prepare(fmt.Sprintf(
		`insert into %s(table1, table2, measure, k, score) values(?, ?, ?, ?, ?);`, DatasetUnionabilityTable))
	if err != nil {
		panic(err)
	}
	//
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int) {
			for s := range scores {
				_, err = stmt.Exec(s.table1, s.table2, s.measure, s.k, s.score)
				if err != nil {
					panic(err)
				}
				progress <- ProgressCounter{1}
			}
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		db.Close()
		stmt.Close()
		close(progress)
	}()
	return progress
}

// Saves the domain pair scores from an input channel to database
// Returns a channel of progress counter
func DoSaveScores(scores <-chan *columnUnionability, fanout int) <-chan ProgressCounter {
	progress := make(chan ProgressCounter)
	wg := &sync.WaitGroup{}
	// output db
	db, err := sql.Open("sqlite3", ColumnUnionabilityDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(
		`drop table if exists %s; create table if not exists %s (
			table1 text,
			column1 integer,
			table2 text,
			column2 integer,
			sarma real,
			sum_cosine real,
			jaccard real,
			containment real);`, ColumnUnionabilityTable, ColumnUnionabilityTable))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db.Prepare(fmt.Sprintf(
		`insert into %s(table1, column1, table2, column2, sarma, sum_cosine, jaccard, containment) values(?, ?, ?, ?, ?, ?, ?, ?);`, ColumnUnionabilityTable))
	if err != nil {
		panic(err)
	}
	//
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int, scores <-chan *columnUnionability) {
			for pairScore := range scores {
				_, err = stmt.Exec(pairScore.Table1, pairScore.Column1, pairScore.Table2, pairScore.Column2, pairScore.Sarma, pairScore.Cosine, pairScore.Jaccard, pairScore.Containment)
				if err != nil {
					panic(err)
				}
				progress <- ProgressCounter{1}
			}
			wg.Done()
		}(i, scores)
	}
	go func() {
		wg.Wait()
		db.Close()
		stmt.Close()
		close(progress)
	}()
	return progress
}

func getDomainWords(file string, index int) []string {
	filepath := path.Join(OutputDir, "domains", file, fmt.Sprintf("%d.values", index))
	f, err := os.Open(filepath)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	var values []string
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		words := wordsFromLine(scanner.Text())
		for _, word := range words {
			values = append(values, normalize(word))
		}
	}
	return values
}

func unique(values []string) []string {
	set := make(map[string]bool)
	var array []string
	for _, v := range values {
		if !set[v] {
			array = append(array, v)
		}
		set[v] = true
	}
	return array
}

func differenceAndIntersection(values1 []string, values2 []string) ([]string, []string) {
	var diff []string
	var intersect []string
	for _, v1 := range values1 {
		found := false
		for _, v2 := range values2 {
			if v1 == v2 {
				found = true
				intersect = append(intersect, v1)
				break
			}
		}
		if !found {
			diff = append(diff, v1)
		}
	}
	return diff, intersect
}

func annotate(values []string) map[string]float64 {
	annotations := make(map[string]float64)
	for _, value := range values {
		words := getWords(value)
		for _, ent := range findEntities(words, lookup, counts) {
			if _, ok := annotations[ent]; !ok {
				annotations[ent] = 1.0
			} else {
				annotations[ent] = annotations[ent] + 1.0
			}
		}
	}
	return annotations
}

func getWords(x string) []string {
	var result []string
	x = strings.ToLower(x)
	y := notLetter.ReplaceAll([]byte(x), space)
	for _, w := range strings.Split(string(y), " ") {
		w = strings.TrimSpace(w)
		if len(w) >= 3 {
			result = append(result, w)
		}
	}
	return result
}

func findEntities(words []string, lookup map[string]map[string]bool, counts map[string]int) []string {
	var ents []string
	var n = len(words)
	if n > 0 {
		for ent, _ := range lookup[words[0]] {
			if countWordEntity(words, ent, lookup) == n && counts[ent] == n {
				ents = append(ents, ent)
			}
		}
	}
	return ents
}

func countWordEntity(words []string, ent string, lookup map[string]map[string]bool) int {
	count := 0
	for _, w := range words {
		if lookup[w][ent] {
			count += 1
		}
	}
	return count
}

func getCosineEmbSum(table1 string, column1 int, table2 string, column2 int) float64 {
	vecFilename1 := filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-sum", table1, column1))
	vecFilename2 := filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-sum", table2, column2))
	if _, err := os.Stat(vecFilename1); err == nil {
		if _, err := os.Stat(vecFilename2); err == nil {
			emb1, err := embedding.ReadVecFromDisk(vecFilename1, binary.BigEndian)
			if err != nil {
				panic(err)
			}
			emb2, err := embedding.ReadVecFromDisk(vecFilename2, binary.BigEndian)
			if err != nil {
				panic(err)
			}
			return embedding.Cosine(emb1, emb2)
		}
	}
	return -1.0
}

// Creates an array of filenames
func GetCODFilenames() []string {
	output := make([]string, 0)
	f, _ := os.Open(OpendataList)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 3)
		filename := path.Join(parts...)
		output = append(output, filename)
	}
	return output
}

// Creates an array of query filenames
func GetQueryFilenames() []string {
	output := make([]string, 0)
	f, _ := os.Open(QueryList)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		filename := scanner.Text()
		output = append(output, filename)
	}

	return output
}
