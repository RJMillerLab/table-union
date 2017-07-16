package main

import (
	"fmt"
	"os"
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

// Take a domain segment, and finds
// relevant entities from the Yago ontology
// using a flexible word-based string matching

type Annotation struct {
	Domain   *Domain
	Entities map[string]bool
}

func DoAnnotateDomainSegment(domain *Domain, yg *yago.Yago) *Annotation {
	// The set of entities found
	annotation := make(map[string]bool)

	// Match unique data values with YAGO entities
	for _, value := range unique(domain.Values) {
		entities := yg.MatchEntity(value, 3)
		for _, entity := range entities {
			annotation[entity] = true
		}
	}
	return &Annotation{domain, annotation}
}

func main() {
	CheckEnv()
	yg := yago.InitYago(Yago_db)

	start := GetNow()
	filenames := StreamFilenames()
	domains := StreamDomainValuesFromFiles(10, filenames)
	fanout := 30
	progress := make(chan *Annotation)

	// Start multiple working threads
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for thread := 0; thread < fanout; thread++ {
		go func(id int, yg *yago.Yago, queue <-chan *Domain, progress chan<- *Annotation) {
			for domain := range queue {
				progress <- DoAnnotateDomainSegment(domain, yg)
			}
			wg.Done()
		}(thread, yg.Copy(), domains, progress)
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
