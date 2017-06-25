package experiment

import (
	"database/sql"
	"fmt"
	"log"
	"path"
	"sync"

	"github.com/RJMillerLab/table-union/benchmarkserver"
)

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
				(query_table, candidate_table, query_col_index, query_col_name,  
				candidate_col_index, candidate_col_name, sim, kunionability, k, n, duration)                   
				values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, measure))
	if err != nil {
		panic(err)
	}
	//
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int) {
			for tableUnion := range alignments {
				for _, pair := range tableUnion.Alignment {
					_, err = stmt.Exec(tableUnion.QueryTableID, tableUnion.CandTableID, pair.QueryColIndex, tableUnion.QueryHeader[pair.QueryColIndex], pair.CandColIndex, tableUnion.CandHeader[pair.CandColIndex], pair.Sim, tableUnion.Kunioability, tableUnion.K, tableUnion.N, tableUnion.Duration)
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

func DoComputeAndSaveExpansion(experimentsDB, experimentsTable, newExperimentsDB, newExperimentsTable, opendataDir string) {
	db1, err := sql.Open("sqlite3", newExperimentsDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db1.Exec(fmt.Sprintf(
		`drop table if exists %s; create table if not exists %s(
			query_table text,
			candidate_table text,
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
			duration real);`, newExperimentsTable, newExperimentsTable))
	if err != nil {
		panic(err)
	}
	// Prepare insert stmt
	stmt, err := db1.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, query_col_index, query_col_name, candidate_col_index, candidate_col_name, sim, kunionability, k, n, query_domain_size, candidate_domain_size, expansion_size, duration) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, newExperimentsTable))
	if err != nil {
		panic(err)
	}
	//
	db2, err := sql.Open("sqlite3", experimentsDB)
	if err != nil {
		panic(err)
	}
	defer db2.Close()
	rows, err := db2.Query(fmt.Sprintf(`SELECT query_table, candidate_table, query_col_index,        query_col_name, candidate_col_index, candidate_col_name, sim, kunionability, k, n, duration FROM %s;`, experimentsTable))
	if err != nil {
		panic(err)
	}
	//
	defer rows.Close()
	var rc int
	for rows.Next() {
		rc += 1
		if rc%100 == 0 {
			log.Printf("Processed %d table profiles.\n", rc)
		}
		var queryTable string
		var candidateTable string
		var queryColIndex int
		var candidateColIndex int
		var queryColName string
		var candidateColName string
		var sim float64
		var kunionability float64
		var k int
		var n int
		var duration float64
		err := rows.Scan(&queryTable, &candidateTable, &queryColIndex, &queryColName, &candidateColIndex, &candidateColName, &sim, &kunionability, &k, &n, &duration)
		if err != nil {
			panic(err)
		}
		queryTableFilename := path.Join(opendataDir, queryTable)
		candidateTableFilename := path.Join(opendataDir, candidateTable)
		expansion := ComputeColumnExpansion(queryTableFilename, candidateTableFilename, queryColIndex, candidateColIndex)
		_, err = stmt.Exec(queryTable, candidateTable, queryColIndex, queryColName, queryColName, candidateColIndex, candidateColName, sim, kunionability, k, n, expansion.Col1NumUniqueValues, expansion.Col2NumUniqueValues, expansion.NumUniqueValuesAdded, duration)
		if err != nil {
			panic(err)
		}
	}
}
