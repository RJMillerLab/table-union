package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"

	. "github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/yago"

	_ "github.com/mattn/go-sqlite3"
)

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

var notLetter = regexp.MustCompile(`[^a-z]+`)
var space = []byte(" ")

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

func countWordEntity(words []string, ent string, lookup map[string]map[string]bool) int {
	count := 0
	for _, w := range words {
		if lookup[w][ent] {
			count += 1
		}
	}
	return count
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

// The section of a domain that
// is not covered with ontology
type PartialAnnotation struct {
	Domain             *Domain
	NotAnnotatedValues map[string]bool
	Entities           map[string]bool
}

func DoFindPartiallyAnnotatedDomainSegment(domain *Domain, yg *yago.Yago) *PartialAnnotation {
	// The set of entities found
	noAnnotation := make(map[string]bool)
	annotation := make(map[string]bool)
	// Get the unique values
	uniqueValues := unique(domain.Values)
	domain.Cardinality = len(uniqueValues)
	domain.Size = len(domain.Values)
	// Match unique data values with YAGO entities
	for _, value := range uniqueValues {
		foundEntities := yg.MatchEntity(value, 3)
		if len(foundEntities) == 0 {
			noAnnotation[value] = true
		}
		for _, entity := range foundEntities {
			annotation[entity] = true
		}
	}
	return &PartialAnnotation{domain, noAnnotation, annotation}
}

func DoFindPartiallyAnnotatedDomainSegmentPlus(domain *Domain, lookup map[string]map[string]bool, counts map[string]int) *PartialAnnotation {
	// The set of entities found
	noAnnotation := make(map[string]bool)
	annotation := make(map[string]bool)
	// Get the unique values
	uniqueValues := unique(domain.Values)
	domain.Cardinality = len(uniqueValues)
	domain.Size = len(domain.Values)
	// updating domain cardinality
	for _, value := range uniqueValues {
		words := getWords(value)
		foundEntities := findEntities(words, lookup, counts)
		if len(foundEntities) == 0 {
			noAnnotation[value] = true
		}
		for _, ent := range foundEntities {
			annotation[ent] = true
		}
	}
	return &PartialAnnotation{domain, noAnnotation, annotation}
}

func main() {
	CheckEnv()
	yg := yago.InitYago(Yago_db)
	//lookup := LoadEntityWords()
	//counts := LoadEntityWordCount()

	start := GetNow()
	filenames := StreamFilenames()
	domains := StreamDomainValuesFromFiles(20, filenames)
	fanout := 10
	progress := make(chan *PartialAnnotation)

	// Start multiple working threads
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for thread := 0; thread < fanout; thread++ {
		go func(id int, yg *yago.Yago, queue <-chan *Domain, progress chan<- *PartialAnnotation) {
			for domain := range queue {
				//progress <- DoFindPartiallyAnnotatedDomainSegmentPlus(domain, lookup, counts)
				log.Printf("domain: %s %d", domain.Filename, domain.Index)
				progress <- DoFindPartiallyAnnotatedDomainSegment(domain, yg)
			}
			wg.Done()
		}(thread, yg.Copy(), domains, progress)
	}
	go func() {
		wg.Wait()
		close(progress)
	}()

	// Save the progress
	segCount := 0
	totalValueCount := 0
	for partialAnnotation := range progress {
		nValues := len(partialAnnotation.NotAnnotatedValues)
		totalValueCount += nValues
		segCount += 1

		if segCount%100 == 0 {
			fmt.Printf("%d segments with %d entities in %.2f seconds\n", segCount, totalValueCount, GetNow()-start)
		}
		if nValues > 0 {
			outputFilename := partialAnnotation.Domain.PhysicalFilename("no-annotation")
			f, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
			for value, _ := range partialAnnotation.NotAnnotatedValues {
				_, err := fmt.Fprintln(f, value)
				if err != nil {
					panic(err)
				}
			}
			f.Close()
		}
		if len(partialAnnotation.Entities) > 0 {
			outputFilename := partialAnnotation.Domain.PhysicalFilename("entities-l0")
			f, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
			for value, _ := range partialAnnotation.Entities {
				_, err := fmt.Fprintln(f, value)
				if err != nil {
					panic(err)
				}
			}
			f.Close()
		}
		cardFilename := partialAnnotation.Domain.PhysicalFilename("ont-noann-card")
		f, err := os.OpenFile(cardFilename, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		fmt.Fprintln(f, len(partialAnnotation.NotAnnotatedValues))
		fmt.Fprintln(f, len(partialAnnotation.Entities))
		fmt.Fprintln(f, partialAnnotation.Domain.Cardinality)
		f.Close()
		cardFilename = partialAnnotation.Domain.PhysicalFilename("card")
		f, err = os.OpenFile(cardFilename, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		fmt.Fprintln(f, partialAnnotation.Domain.Cardinality)
		f.Close()
		//sizeFilename := partialAnnotation.Domain.PhysicalFilename("size")
		//f, err = os.OpenFile(sizeFilename, os.O_CREATE|os.O_WRONLY, 0644)
		//if err != nil {
		//	panic(err)
		//}
		//fmt.Fprintln(f, partialAnnotation.Domain.Size)
		//f.Close()
	}
	fmt.Printf("%d segments with %d values in %.2f seconds\n", segCount, totalValueCount, GetNow()-start)

}
