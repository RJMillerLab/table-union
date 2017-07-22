package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"

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
	filenames := StreamFilenames()
	statsTable := "card_stats"
	statsDB := "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark/stats.sqlite"
	// create db
	db, err := sql.Open("sqlite3", statsDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s(table_name text, column_index int,no_ontology_num int, cardinality int, num_entities int);`, statsTable, statsTable))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(table_name, column_index, no_ontology_num, cardinality, num_entities) values (?,?,?,?,?);`, statsTable))
	if err != nil {
		panic(err)
	}
	domainDir := "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark"
	for tableID := range filenames {
		for _, index := range getTextDomains(domainDir, tableID) {
			ontcardFilename := path.Join(domainDir, "domains", tableID, fmt.Sprintf("%d.%s", index, "ont-noann-card"))
			f, err := os.Open(ontcardFilename)
			if err != nil {
				panic(err)
			}
			scanner := bufio.NewScanner(f)
			lineInx := 0
			card := 0
			numEntities := 0
			noOntcoverage := 0
			for scanner.Scan() {
				if lineInx == 0 {
					noOntcoverage, _ = strconv.Atoi(scanner.Text())
					lineInx += 1
				}
				if lineInx == 1 {
					numEntities, _ = strconv.Atoi(scanner.Text())
					lineInx += 1
				}
				if lineInx == 2 {
					card, _ = strconv.Atoi(scanner.Text())
				}
			}
			_, err = stmt.Exec(tableID, index, noOntcoverage, card, numEntities)
			if err != nil {
				panic(err)
			}
			f.Close()
		}
	}
	db.Close()
}

func getTextDomains(outputDir, file string) (indices []int) {
	typesFile := path.Join(outputDir, "domains", file, "types")
	f, err := os.Open(typesFile)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		if len(parts) == 2 {
			index, err := strconv.Atoi(parts[0])
			if err != nil {
				log.Printf("error in types of file: %s", file)
				panic(err)
			}
			if parts[1] == "text" {
				indices = append(indices, index)
			}
		} else {
			log.Printf("get text domains not 2: %v %s", parts, file)
		}
	}
	return
}
