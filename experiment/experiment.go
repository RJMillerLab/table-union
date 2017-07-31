package experiment

import (
	"database/sql"
	"fmt"
	"log"
	"path"
	"sync"

	"github.com/RJMillerLab/table-union/benchmarkserver"
)

type columnPair struct {
	queryTable        string
	candidateTable    string
	queryColIndex     int
	candidateColIndex int
}

type tablePair struct {
	queryTable     string
	candidateTable string
}

type tableAlignment struct {
	queryTable     string
	candidateTable string
	matches        map[int]int
}

type columnExpansion struct {
	columns              columnPair
	col1NumUniqueValues  int
	col2NumUniqueValues  int
	numUniqueValuesAdded int
}

type rowExpansion struct {
	alignment        tableAlignment
	queryNumRows     int
	candidateNumRows int
	numRowsAdded     int
}

type ProgressCounter struct {
	Values int
}

func GetQueryPath(dir, queryFilename string) string {
	return path.Join(dir, queryFilename)
}

func DoSaveAlignments(alignments <-chan benchmarkserver.Union, measure, experimentsDB string, fanout int) <-chan ProgressCounter {
	progress := make(chan ProgressCounter)
	wg := &sync.WaitGroup{}
	// output db
	db, err := sql.Open("sqlite3", experimentsDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(
		`drop table if exists %s;
		create table if not exists %s (
			query_table text,
			candidate_table text,
			num_query_columns int,
			num_query_text_columns int,
			num_candidate_columns int,
			num_candidate_text_columns int,
			query_col_index int,
			query_col_name text,
			candidate_col_index int,
			candidate_col_name text,
			jaccard real, 
			hypergeometric real,
			ontology_jaccard real,
			ontology_hypergeometric real,
			cosine real,
			f_distribution real,
			t2 real, 
			sim real,
			kunionability real,
			k int,
			n int,
			expansion int,
			duration real, 
			containment real,
			query_cardinality int, 
			candidate_cardinality int);`, measure, measure))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db.Prepare(fmt.Sprintf(
		`insert into %s
				(query_table, candidate_table, num_query_columns, num_query_text_columns, num_candidate_columns, num_candidate_text_columns, query_col_index, query_col_name,  
				candidate_col_index, candidate_col_name, sim, kunionability, k, n, jaccard, hypergeometric, ontology_jaccard, ontology_hypergeometric, cosine, f_distribution, t2, duration, containment,query_cardinality, candidate_cardinality)                   
				values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, measure))
	if err != nil {
		panic(err)
	}
	//
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int) {
			for tableUnion := range alignments {
				for _, pair := range tableUnion.Alignment {
					_, err = stmt.Exec(tableUnion.QueryTableID, tableUnion.CandTableID, len(tableUnion.QueryHeader), len(tableUnion.QueryTextHeader), len(tableUnion.CandHeader), len(tableUnion.CandTextHeader), pair.QueryColIndex, tableUnion.QueryHeader[pair.QueryColIndex], pair.CandColIndex, tableUnion.CandHeader[pair.CandColIndex], pair.Sim, tableUnion.Kunioability, tableUnion.K, tableUnion.N, pair.Jaccard, pair.Hypergeometric, pair.OntologyJaccard, pair.OntologyHypergeometric, pair.Cosine, pair.F, pair.T2, tableUnion.Duration, pair.Containment, pair.QueryCardinality, pair.CandCardinality)
					if err != nil {
						panic(err)
					}
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

func computeUnionPairRowExpansion(tablePairs <-chan tableAlignment, fanout int) <-chan rowExpansion {
	out := make(chan rowExpansion)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func() {
			for pair := range tablePairs {
				queryTableFilename := path.Join(OpendataDir, pair.queryTable)
				candidateTableFilename := path.Join(OpendataDir, pair.candidateTable)
				expansion := ComputeRowExpansion(queryTableFilename, candidateTableFilename, pair.matches)
				out <- rowExpansion{
					alignment:        pair,
					queryNumRows:     expansion.T1NumRows,
					candidateNumRows: expansion.T2NumRows,
					numRowsAdded:     expansion.NumRowsAdded,
				}
				log.Printf("Expansion is %d", expansion.NumRowsAdded)
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func computeUnionPairExpansion(columnPairs <-chan columnPair, fanout int) <-chan columnExpansion {
	out := make(chan columnExpansion)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func() {
			for pair := range columnPairs {
				queryTableFilename := path.Join(OpendataDir, pair.queryTable)
				candidateTableFilename := path.Join(OpendataDir, pair.candidateTable)
				if selfUnion(pair) {
					out <- columnExpansion{
						columns: pair,
					}
				} else {
					expansion := ComputeColumnExpansion(queryTableFilename, candidateTableFilename, pair.queryColIndex, pair.candidateColIndex)
					out <- columnExpansion{
						columns:              pair,
						col1NumUniqueValues:  expansion.Col1NumUniqueValues,
						col2NumUniqueValues:  expansion.Col2NumUniqueValues,
						numUniqueValuesAdded: expansion.NumUniqueValuesAdded,
					}
					log.Printf("Expansion is %d", expansion.NumUniqueValuesAdded)
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func selfUnion(pair columnPair) bool {
	return pair.queryTable == pair.candidateTable && pair.queryColIndex == pair.candidateColIndex
}

func readAlignments() <-chan tableAlignment {
	log.Printf("started reading alignments.")
	log.Printf("experimentdb: %s", ExperimentDB)
	log.Printf("experimenttable: %s", ExperimentTable)
	out := make(chan tableAlignment, 100)
	tablePairs := make([]tablePair, 0)
	// reading table pairs
	db2, err := sql.Open("sqlite3", ExperimentDB)
	if err != nil {
		panic(err)
	}
	rows, err := db2.Query(fmt.Sprintf(`SELECT DISTINCT query_table, candidate_table FROM %s;`, ExperimentTable))
	if err != nil {
		panic(err)
	}
	//
	for rows.Next() {
		var queryTable string
		var candidateTable string
		err := rows.Scan(&queryTable, &candidateTable)
		if err != nil {
			panic(err)
		}
		p := tablePair{
			queryTable:     queryTable,
			candidateTable: candidateTable,
		}
		tablePairs = append(tablePairs, p)
	}
	rows.Close()
	db2.Close()
	go func() {
		db2, err := sql.Open("sqlite3", ExperimentDB)
		if err != nil {
			panic(err)
		}
		defer db2.Close()
		for _, p := range tablePairs {
			rows, err := db2.Query(fmt.Sprintf(`SELECT DISTINCT query_col_index, candidate_col_index FROM %s WHERE query_table = "%s" AND candidate_table = "%s";`, ExperimentTable, p.queryTable, p.candidateTable))
			if err != nil {
				panic(err)
			}
			//
			defer rows.Close()
			matches := make(map[int]int)
			for rows.Next() {
				var queryColIndex int
				var candidateColIndex int
				err := rows.Scan(&queryColIndex, &candidateColIndex)
				if err != nil {
					panic(err)
				}
				matches[queryColIndex] = candidateColIndex
			}
			out <- tableAlignment{
				queryTable:     p.queryTable,
				candidateTable: p.candidateTable,
				matches:        matches,
			}
		}
		close(out)
	}()
	log.Printf("Finished reading column pairs.")
	return out
}

func readColumnPairs() <-chan columnPair {
	log.Printf("started reading column pairs.")
	out := make(chan columnPair, 100)
	go func() {
		db2, err := sql.Open("sqlite3", SharedDB)
		if err != nil {
			panic(err)
		}
		defer db2.Close()
		rows, err := db2.Query(fmt.Sprintf(`SELECT DISTINCT query_table, candidate_table, query_col_index, candidate_col_index FROM %s;`, SharedTable))
		if err != nil {
			panic(err)
		}
		//
		defer rows.Close()
		for rows.Next() {
			var queryTable string
			var candidateTable string
			var queryColIndex int
			var candidateColIndex int
			err := rows.Scan(&queryTable, &candidateTable, &queryColIndex, &candidateColIndex)
			if err != nil {
				panic(err)
			}
			out <- columnPair{
				queryTable:        queryTable,
				candidateTable:    candidateTable,
				queryColIndex:     queryColIndex,
				candidateColIndex: candidateColIndex,
			}
		}
		close(out)
	}()
	log.Printf("Finished reading column pairs.")
	return out
}

func DoComputeAndSaveRowExpansion() {
	alignments := readAlignments()
	expansions := computeUnionPairRowExpansion(alignments, 5)
	progress := saveRowExpansion(expansions)
	i := 0
	total := ProgressCounter{}
	start := GetNow()
	tick := GetNow()
	for n := range progress {
		total.Values += n.Values
		i += 1
		now := GetNow()

		if now-tick > 50 {
			tick = now
			fmt.Printf("Computed and saved the expansion of %d unionable columns in %.2f seconds\n", total.Values, now-start)
		}
	}
	fmt.Printf("Computed and saved the expansion of %d unionable columns in %.2f seconds\n", total.Values, GetNow()-start)
	log.Printf("Done computing and saving expansion!")
}

func DoComputeAndSaveExpansion() {
	columnPairs := readColumnPairs()
	expansions := computeUnionPairExpansion(columnPairs, 5)
	progress := saveExpansion(expansions)
	i := 0
	total := ProgressCounter{}
	start := GetNow()
	tick := GetNow()
	for n := range progress {
		total.Values += n.Values
		i += 1
		now := GetNow()

		if now-tick > 50 {
			tick = now
			fmt.Printf("Computed and saved the expansion of %d unionable columns in %.2f seconds\n", total.Values, now-start)
		}
	}
	fmt.Printf("Computed and saved the expansion of %d unionable columns in %.2f seconds\n", total.Values, GetNow()-start)
	log.Printf("Done computing and saving expansion!")
}

func saveRowExpansion(expansions <-chan rowExpansion) <-chan ProgressCounter {
	out := make(chan ProgressCounter)
	db, err := sql.Open("sqlite3", ExpansionDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s(query_table text,  candidate_table text, query_row_num int, candidate_row_num int, row_expansion_size int);`, ExpansionTable, ExpansionTable))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, query_row_num, candidate_row_num, row_expansion_size) values(?, ?, ?, ?, ?);`, ExpansionTable))
	if err != nil {
		panic(err)
	}
	go func() {
		for expansion := range expansions {
			_, err = stmt.Exec(expansion.alignment.queryTable, expansion.alignment.candidateTable, expansion.queryNumRows, expansion.candidateNumRows, expansion.numRowsAdded)
			if err != nil {
				panic(err)
			}
			out <- ProgressCounter{1}
		}
		db.Close()
		close(out)
	}()
	return out
}

func saveExpansion(expansions <-chan columnExpansion) <-chan ProgressCounter {
	out := make(chan ProgressCounter)
	db, err := sql.Open("sqlite3", ExpansionDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s(query_table text,  candidate_table text, query_col_index int,candidate_col_index int,query_domain_size int, candidate_domain_size int, expansion_size int);`, ExpansionTable, ExpansionTable))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, query_col_index, candidate_col_index, query_domain_size, candidate_domain_size, expansion_size) values(?, ?, ?, ?, ?, ?, ?);`, ExpansionTable))
	if err != nil {
		panic(err)
	}
	go func() {
		for expansion := range expansions {
			_, err = stmt.Exec(expansion.columns.queryTable, expansion.columns.candidateTable, expansion.columns.queryColIndex, expansion.columns.candidateColIndex, expansion.col1NumUniqueValues, expansion.col2NumUniqueValues, expansion.numUniqueValuesAdded)
			if err != nil {
				panic(err)
			}
			out <- ProgressCounter{1}
		}
		db.Close()
		close(out)
	}()
	return out
}
