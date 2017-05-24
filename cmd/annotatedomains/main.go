package main

import (
	"fmt"
	. "opendata"
	"os"
	"regexp"
	"strings"
	"sync"

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

// Take a domain segment, and finds
// relevant entities from the Yago ontology
// using a flexible word-based string matching

type Annotation struct {
	Domain   *Domain
	Entities map[string]bool
}

func DoAnnotateDomainSegment(domain *Domain, lookup map[string]map[string]bool, counts map[string]int) *Annotation {
	// The set of entities found
	annotation := make(map[string]bool)

	// Get the unique values
	for _, value := range unique(domain.Values) {
		words := getWords(value)
		for _, ent := range findEntities(words, lookup, counts) {
			annotation[ent] = true
		}
	}

	return &Annotation{domain, annotation}
}

func main() {
	CheckEnv()
	lookup := LoadEntityWords()
	counts := LoadEntityWordCount()

	start := GetNow()
	filenames := StreamFilenames()
	domains := StreamDomainValuesFromFiles(10, filenames)
	fanout := 30
	progress := make(chan *Annotation)

	// Start multiple working threads
	wg := &sync.WaitGroup{}
	for thread := 0; thread < fanout; thread++ {
		wg.Add(1)
		go func(id int, queue <-chan *Domain, progress chan<- *Annotation) {
			for domain := range queue {
				progress <- DoAnnotateDomainSegment(domain, lookup, counts)
			}
			wg.Done()
		}(thread, domains, progress)
	}

	// Save the progress
	go func() {
		segCount := 0
		totalValueCount := 0
		for annotation := range progress {
			nEntities := len(annotation.Entities)
			totalValueCount += nEntities
			segCount += 1

			if segCount%100 == 0 {
				fmt.Printf("%d segments with %d entities in %.2f seconds\n", segCount, totalValueCount, GetNow()-start)
			}

			if nEntities > 0 {
				output_filename := annotation.Domain.PhysicalFilename("entities")
				f, err := os.OpenFile(output_filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
				if err != nil {
					panic(err)
				}
				for entity, _ := range annotation.Entities {
					fmt.Fprintln(f, entity)
				}
				f.Close()
			}
		}
		fmt.Printf("%d segments with %d values in %.2f seconds\n", segCount, totalValueCount, GetNow()-start)
	}()

	// Wait for the threads to finish processing
	// their individual queues
	wg.Wait()
	close(progress)
}
