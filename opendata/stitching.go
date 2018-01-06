package opendata

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
)

type StitchedTable struct {
	queryTable string
	candTable  string
	alignment  []string
	score      float64
}

func ComputeTableStitchingUnionability() {
	tableUnions := make(chan StitchedTable)
	db, err := sql.Open("sqlite3", AttStitchingDB)
	if err != nil {
		panic(err)
	}
	rows2, err := db.Query(fmt.Sprintf(`SELECT DISTINCT query_table, candidate_table FROM %s;`, AttStitchingTable))
	if err != nil {
		panic(err)
	}
	//
	tablePairs := make(chan string)
	wwg := &sync.WaitGroup{}
	wwg.Add(3)
	go func() {
		for rows2.Next() {
			var queryTable string
			var candidateTable string
			err := rows2.Scan(&queryTable, &candidateTable)
			if err != nil {
				panic(err)
			}
			tablePairs <- queryTable + " " + candidateTable
		}
		close(tablePairs)
		wwg.Done()
	}()
	wg := &sync.WaitGroup{}
	wg.Add(35)
	for i := 0; i < 35; i++ {
		go func() {
			for pair := range tablePairs {
				alignment := make([]string, 0)
				rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT query_col_name, candidate_col_name, MAX(score) AS maxScore FROM %s WHERE query_table='%s' AND candidate_table='%s' AND score > 0.0 GROUP BY query_col_name, candidate_col_name ORDER BY maxScore desc;`, AttStitchingTable, strings.Split(pair, " ")[0], strings.Split(pair, " ")[1]))
				if err != nil {
					panic(err)
				}
				unionabilityScore := 0.0
				queryAligned := make(map[string]bool)
				candAligned := make(map[string]bool)
				c := 1
				for rows.Next() {
					var maxScore float64
					var queryColumn string
					var candidateColumn string
					err := rows.Scan(&queryColumn, &candidateColumn, &maxScore)
					if err != nil {
						panic(err)
					}
					if c == 1 {
						unionabilityScore = maxScore
						queryAligned[queryColumn] = true
						candAligned[candidateColumn] = true
						alignment = append(alignment, queryColumn+"@FN@"+candidateColumn)
						c += 1
					} else if _, ok := queryAligned[queryColumn]; !ok {
						if _, ok := candAligned[candidateColumn]; !ok {
							unionabilityScore += maxScore
							queryAligned[queryColumn] = true
							candAligned[candidateColumn] = true
							alignment = append(alignment, queryColumn+"@FN@"+candidateColumn)
							c += 1
						}
					}
				}
				cus := StitchedTable{
					queryTable: strings.Split(pair, " ")[0],
					candTable:  strings.Split(pair, " ")[1],
					score:      unionabilityScore,
					alignment:  alignment,
				}
				tableUnions <- cus
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		db.Close()
		close(tableUnions)
		wwg.Done()
	}()
	go func() {
		saveTableUnionability(tableUnions)
		wwg.Done()
	}()
	wwg.Wait()
}

func saveTableUnionability(unions chan StitchedTable) {
	db, err := sql.Open("sqlite3", TableStitchingDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (query_table text, candidate_table text, query_col_name text, candidate_col_name text, score real);`, TableStitchingTable, TableStitchingTable))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, query_col_name, candidate_col_name, score) values(?, ?, ?, ?, ?);`, TableStitchingTable))
	if err != nil {
		panic(err)
	}
	count := 0
	for cus := range unions {
		queryTable := cus.queryTable
		candidateTable := cus.candTable
		for _, a := range cus.alignment {
			parts := strings.Split(a, "@FN@")
			_, err = stmt.Exec(queryTable, candidateTable, parts[0], parts[1], cus.score)
			if err != nil {
				panic(err)
			}
		}
		count += 1
		if count%500 == 0 {
			log.Printf("Processed %d table pairs.", count)
		}
	}
	db.Close()
}
