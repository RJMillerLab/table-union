package benchmark

import (
	"database/sql"
	"encoding/binary"
	"fmt"

	"github.com/RJMillerLab/table-union/embedding"
)

var (
	TableName = "benchmark"
	ByteOrder = binary.BigEndian
)

type SamplePair struct {
	ID1   string
	ID2   string
	Vec1  []float64
	Vec2  []float64
	Label int
}

func WriteToDB(samplePairs <-chan *SamplePair, sqliteDB string) {
	db, err := sql.Open("sqlite3", sqliteDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	_, err = db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			id1 TEXT,
			id2 TEXT,
			vec1 BLOB,
			vec2 BLOB,
			label INTEGER
		);
		`, TableName))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`
		INSERT INTO %s(id1, id2, vec1, vec2, label) VALUES(?, ?, ?, ?, ?);
		`, TableName))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	for p := range samplePairs {
		binVec1 := embedding.VecToBytes(p.Vec1, ByteOrder)
		binVec2 := embedding.VecToBytes(p.Vec2, ByteOrder)
		if _, err := stmt.Exec(p.ID1, p.ID2, binVec1, binVec2, p.Label); err != nil {
			panic(err)
		}
	}
}
