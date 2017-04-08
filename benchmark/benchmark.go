package benchmark

import (
	"database/sql"
	"encoding/binary"
	"fmt"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/ekzhu/counter"
)

var (
	ColumnPairTableName = "column_pair"
	VecTableName        = "domain_vec"
	ByteOrder           = binary.BigEndian
)

type SamplePair struct {
	TableID1     string
	TableID2     string
	ColumnIndex1 int
	ColumnIndex2 int
	Vec1         []float64
	Vec2         []float64
	Label        int
}

func (p *SamplePair) ColumnIDs() (id1, id2 string) {
	id1 = fmt.Sprintf("%s_%d", p.TableID1, p.ColumnIndex1)
	id2 = fmt.Sprintf("%s_%d", p.TableID2, p.ColumnIndex2)
	return
}

func WriteToDB(samplePairs <-chan *SamplePair, sqliteDB string) {
	db, err := sql.Open("sqlite3", sqliteDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			table_id TEXT,
			column_index INTEGER,
			vec BLOB
		);
		`, VecTableName))
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			table_id1 TEXT,
			column_index1 INTEGER,
			table_id2 TEXT,
			column_index2 INTEGER,
			label INTEGER
		);
		`, ColumnPairTableName))
	if err != nil {
		panic(err)
	}
	// Prepare statements
	columnPairStmt, err := db.Prepare(fmt.Sprintf(`
		INSERT INTO %s(table_id1, column_index1, table_id2, column_index2, label) VALUES(?, ?, ?, ?, ?);
		`, ColumnPairTableName))
	if err != nil {
		panic(err)
	}
	defer columnPairStmt.Close()
	domainVecStmt, err := db.Prepare(fmt.Sprintf(`
		INSERT INTO %s(table_id, column_index, vec) VALUES(?, ?, ?);
		`, VecTableName))
	if err != nil {
		panic(err)
	}
	defer domainVecStmt.Close()

	// Insert into tables
	columnInserted := counter.NewCounter()
	var count int
	for p := range samplePairs {
		// Insert into domain vec table
		id1, id2 := p.ColumnIDs()
		if !columnInserted.Has(id1) {
			binVec1 := embedding.VecToBytes(p.Vec1, ByteOrder)
			if _, err := domainVecStmt.Exec(p.TableID1, p.ColumnIndex1, binVec1); err != nil {
				panic(err)
			}
			columnInserted.Update(id1)
		}
		if !columnInserted.Has(id2) {
			binVec2 := embedding.VecToBytes(p.Vec2, ByteOrder)
			if _, err := domainVecStmt.Exec(p.TableID2, p.ColumnIndex2, binVec2); err != nil {
				panic(err)
			}
			columnInserted.Update(id2)
		}
		// Insert into column pair table
		if _, err := columnPairStmt.Exec(p.TableID1, p.ColumnIndex1, p.TableID2, p.ColumnIndex2, p.Label); err != nil {
			panic(err)
		}
		count++
		fmt.Printf("\rInserted %d pairs", count)
	}
	fmt.Println()

	// Build index on vector table
	_, err = db.Exec(fmt.Sprintf(`
		CREATE INDEX ind_column_id ON %s(table_id, column_index);
		`, VecTableName))
	if err != nil {
		panic(err)
	}
}
