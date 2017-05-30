package ontology

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

var entityFile = "/home/kenpu/TABLE_UNION_OUTPUT/word-entity.txt"
var notLetter = regexp.MustCompile(`[^a-z]+`)
var space = []byte(" ")
var yagoDB = "/home/kenpu/TABLE_UNION_OUTPUT/yago.sqlite3.0"

type Annotator struct {
	enntityWords     map[string]map[string]bool
	entityWordCounts map[string]int
}

func NewAnnotator() *Annotator {
	return &Annotator{
		enntityWords:     loadEntityWords(),
		entityWordCounts: loadEntityWordCount(),
	}
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

func loadEntityWords() map[string]map[string]bool {
	lookup := make(map[string]map[string]bool)
	f, err := os.Open(entityFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	i := 0
	start := getNow()

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "|", 2)
		word, entity := parts[0], parts[1]
		if _, ok := lookup[word]; !ok {
			lookup[word] = make(map[string]bool)
		}
		lookup[word][entity] = true
		i += 1
		if i%100000 == 0 {
			fmt.Printf("LoadEntityWords: %d in %.2f seconds\n", i, getNow()-start)
		}
	}
	return lookup
}

func loadEntityWordCount() map[string]int {
	counts := make(map[string]int)
	db, err := sql.Open("sqlite3", yagoDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(`select entity, words_count from words_count`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var ent string
		var count int
		if err = rows.Scan(&ent, &count); err != nil {
			panic(err)
		}
		counts[ent] = count
		i += 1
		if i%100000 == 0 {
			fmt.Printf("LoadEntityWordCount: %d\n", i)
		}
	}
	return counts
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

func (a *Annotator) DoAnnotateDomain(domain []string) []string {
	lookup := a.enntityWords
	counts := a.entityWordCounts
	// The set of entities found
	annotation := make(map[string]bool)

	// Get the unique values
	for _, value := range unique(domain) {
		words := getWords(value)
		for _, ent := range findEntities(words, lookup, counts) {
			annotation[ent] = true
		}
	}

	entities := make([]string, 0)
	for k, _ := range annotation {
		entities = append(entities, k)
	}
	return entities
}

func getNow() float64 {
	return float64(time.Now().UnixNano()) / 1E9
}
