package opendata

import (
	"bufio"
	"encoding/binary"
	"fmt"
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
	lookup    map[string]map[string]bool
	counts    map[string]int
	notLetter = regexp.MustCompile(`[^a-z]+`)
	space     = []byte(" ")
)

type OntUnionability struct {
	Table1      string
	Column1     int
	Table2      string
	Column2     int
	Sarma       int
	Jaccard     float64
	Containment float64
	Cosine      float64
}

func Init() {
	lookup = LoadEntityWords()
	counts = LoadEntityWordCount()
}

func DoComputeUnionability(queryFilename string, files <-chan string, fanout int) <-chan *OntUnionability {
	out := make(chan *OntUnionability)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int) {
			for _, col1 := range getTextDomains(queryFilename) {
				for file := range files {
					dcount := 0
					for _, col2 := range getTextDomains(file) {
						dcount += 1
						sarma, jaccard, containment, cosine := unionability(queryFilename, col1, file, col2)
						out <- &OntUnionability{
							Table1:      queryFilename,
							Column1:     col1,
							Table2:      file,
							Column2:     col2,
							Sarma:       sarma,
							Jaccard:     jaccard,
							Containment: containment,
							Cosine:      cosine,
						}
					}
				}
			}
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func unionability(table1 string, column1 int, table2 string, column2 int) (int, float64, float64, float64) {
	domain1Values := unique(getDomainWords(table1, column1))
	domain2Values := unique(getDomainWords(table2, column2))
	difference, intersection := differenceAndIntersection(domain2Values, domain1Values)
	// computing scores
	sarma := getSarma(domain1Values, difference)
	jaccard := getJaccard(domain1Values, domain1Values, intersection)
	containment := getContainment(domain1Values, domain1Values, intersection)
	cosine := getCosineEmbSum(table1, column1, table2, column2)
	return sarma, jaccard, containment, cosine
}

func getSarma(query, expansion []string) int {
	annotations1 := annotate(query)
	annotations2 := annotate(expansion)
	sarma := 0
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

// Saves the domain pair scores from an input channel to disk
// Returns a channel of progress counter
func DoSaveScores(scores <-chan *OntUnionability, sarmaFilename, jaccardFilename, containmentFilename, cosineFilename string, fanout int) <-chan ProgressCounter {
	progress := make(chan ProgressCounter)
	wg := &sync.WaitGroup{}
	f1, err := os.OpenFile(sarmaFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	f2, err := os.OpenFile(jaccardFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	f3, err := os.OpenFile(containmentFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	f4, err := os.OpenFile(cosineFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int, scores <-chan *OntUnionability) {
			for pairScore := range scores {
				_, err := f1.WriteString(pairScore.Table1 + "," + strconv.Itoa(pairScore.Column1) + "," + pairScore.Table2 + "," + strconv.Itoa(pairScore.Column2) + "," + strconv.Itoa(pairScore.Sarma) + "\n")
				if err != nil {
					panic(err)
				}
				_, err = f2.WriteString(pairScore.Table1 + "," + strconv.Itoa(pairScore.Column1) + "," + pairScore.Table2 + "," + strconv.Itoa(pairScore.Column2) + "," + strconv.FormatFloat(pairScore.Jaccard, 'f', 6, 64) + "\n")
				if err != nil {
					panic(err)
				}
				_, err = f3.WriteString(pairScore.Table1 + "," + strconv.Itoa(pairScore.Column1) + "," + pairScore.Table2 + "," + strconv.Itoa(pairScore.Column2) + "," + strconv.FormatFloat(pairScore.Containment, 'f', 6, 64) + "\n")
				if err != nil {
					panic(err)
				}
				//_, err = f4.WriteString(pairScore.Table1 + "," + strconv.Itoa(pairScore.Column1) + "," + pairScore.Table2 + "," + strconv.Itoa(pairScore.Column2) + "," + strconv.FormatFloat(pairScore.Cosine, 'f', 6, 64) + "\n")
				//if err != nil {
				//	panic(err)
				//}
				progress <- ProgressCounter{1}
			}
			wg.Done()
		}(i, scores)
	}
	go func() {
		wg.Wait()
		f1.Close()
		f2.Close()
		f3.Close()
		f4.Close()
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
		if !found {
			for _, v2 := range values2 {
				if v1 == v2 {
					found = true
					intersect = append(intersect, v1)
				}
			}
		}
		if !found {
			diff = append(diff, v1)
		}
	}
	return diff, intersect
}

func annotate(values []string) map[string]int {
	annotations := make(map[string]int)
	for _, value := range values {
		words := getWords(value)
		for _, ent := range findEntities(words, lookup, counts) {
			if _, ok := annotations[ent]; !ok {
				annotations[ent] = 1
			} else {
				annotations[ent] = annotations[ent] + 1
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
	return 1.0
	vecFilename1 := filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-sum", table1, column1))
	vecFilename2 := filepath.Join(OutputDir, "domains", fmt.Sprintf("%s/%d.ft-sum", table2, column2))
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
