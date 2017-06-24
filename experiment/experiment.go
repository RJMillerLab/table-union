package experiment

import (
	"database/sql"
	"fmt"
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
					_, err = stmt.Exec(tableUnion.QueryTableID, tableUnion.CandTableID, pair.QueryColIndex, tableUnion.QueryTextHeader[pair.QueryColIndex], pair.CandColIndex, tableUnion.CandHeader[pair.CandColIndex], pair.Sim, tableUnion.Kunioability, tableUnion.K, tableUnion.N, tableUnion.Duration)
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
