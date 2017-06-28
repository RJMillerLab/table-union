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

type columnExpansion struct {
	columns              columnPair
	col1NumUniqueValues  int
	col2NumUniqueValues  int
	numUniqueValuesAdded int
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
		`drop table if exists %s; create table if not exists %s (
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
			sim real,
			kunionability real,
			k int,
			n int,
			expansion int,
			duration real);`, measure, measure))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db.Prepare(fmt.Sprintf(
		`insert into %s
				(query_table, candidate_table, num_query_columns, num_query_text_columns, num_candidate_columns, num_candidate_text_columns, query_col_index, query_col_name,  
				candidate_col_index, candidate_col_name, sim, kunionability, k, n, duration)                   
				values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, measure))
	if err != nil {
		panic(err)
	}
	//
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int) {
			for tableUnion := range alignments {
				for _, pair := range tableUnion.Alignment {
					_, err = stmt.Exec(tableUnion.QueryTableID, tableUnion.CandTableID, len(tableUnion.QueryHeader), len(tableUnion.QueryTextHeader), len(tableUnion.CandHeader), len(tableUnion.CandTextHeader), pair.QueryColIndex, tableUnion.QueryHeader[pair.QueryColIndex], pair.CandColIndex, tableUnion.CandHeader[pair.CandColIndex], pair.Sim, tableUnion.Kunioability, tableUnion.K, tableUnion.N, tableUnion.Duration)
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

func saveExpansion(expansions <-chan columnExpansion) <-chan ProgressCounter {
	out := make(chan ProgressCounter)
	db1, err := sql.Open("sqlite3", ExpansionDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db1.Exec(fmt.Sprintf(
		`drop table if exists %s; create table if not exists %s(
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
			sim real,
			kunionability real,
			k int,
			n int,
			query_domain_size int,
			candidate_domain_size int, 
			expansion_size int,
			duration real);`, ExpansionTable, ExpansionTable))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db1.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, num_query_columns, num_query_text_columns, num_candidate_columns, num_candidate_text_columns, query_col_index, query_col_name, candidate_col_index, candidate_col_name, sim, kunionability, k, n, query_domain_size, candidate_domain_size, expansion_size, duration) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, ExpansionTable))
	if err != nil {
		panic(err)
	}
	go func() {
		db2, err := sql.Open("sqlite3", ExperimentDB)
		if err != nil {
			panic(err)
		}
		for expansion := range expansions {
			rows, err := db2.Query(fmt.Sprintf(`SELECT query_table, candidate_table, num_query_columns, num_query_text_columns, num_candidate_columns, num_candidate_text_columns, query_col_index,                  query_col_name, candidate_col_index, candidate_col_name, sim, kunionability, k, n, duration FROM %s where   query_table="%s" AND query_col_index=%d AND candidate_table="%s" AND candidate_col_index=%d;`, ExperimentTable, expansion.columns.queryTable, expansion.columns.candidateColIndex, expansion.columns.candidateTable, expansion.columns.candidateColIndex))
			if err != nil {
				panic(err)
			}
			//
			//defer rows.Close()
			for rows.Next() {
				var queryTable string
				var candidateTable string
				var queryColIndex int
				var numQueryColumns int
				var numQueryTextColumns int
				var numCandidateColumns int
				var numCandidateTextColumns int
				var candidateColIndex int
				var queryColName string
				var candidateColName string
				var sim float64
				var kunionability float64
				var k int
				var n int
				var duration float64
				err := rows.Scan(&queryTable, &candidateTable, &numQueryColumns, &numQueryTextColumns, &numCandidateColumns, &numCandidateTextColumns, &queryColIndex, &queryColName, &candidateColIndex, &candidateColName, &sim, &kunionability, &k, &n, &duration)
				if err != nil {
					panic(err)
				}
				_, err = stmt.Exec(queryTable, candidateTable, numQueryColumns, numQueryTextColumns, numCandidateColumns, numCandidateTextColumns, queryColIndex, queryColName, candidateColIndex, candidateColName, sim, kunionability, k, n, expansion.col1NumUniqueValues, expansion.col2NumUniqueValues, expansion.numUniqueValuesAdded, duration)
				if err != nil {
					panic(err)
				}
				out <- ProgressCounter{1}
			}
			rows.Close()
		}
		db1.Close()
		db2.Close()
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

func readColumnPairs() <-chan columnPair {
	log.Printf("started reading column pairs.")
	out := make(chan columnPair, 100)
	go func() {
		db2, err := sql.Open("sqlite3", ExperimentDB)
		if err != nil {
			panic(err)
		}
		defer db2.Close()
		rows, err := db2.Query(fmt.Sprintf(`SELECT DISTINCT query_table, candidate_table, query_col_index, candidate_col_index FROM %s;`, ExperimentTable))
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

func DoComputeAndSaveExpansion() {
	columnPairs := readColumnPairs()
	expansions := computeUnionPairExpansion(columnPairs, 45)
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
