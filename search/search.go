package search

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	"github.com/RJMillerLab/lsh"
	fasttext "github.com/ekzhu/go-fasttext"
)

const (
	TableName = "search_index"
)

var (
	ErrNoEmbFound = errors.New("No embedding found")
	ByteOrder     = binary.BigEndian
)

type EmbEntry struct {
	TableID     string    `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	Vec         []float64 `json:"vec"`
}

type SearchIndex struct {
	ft        *fasttext.FastText
	db        *sql.DB // Sqlite store for the embedding entries
	lsh       *lsh.CosineLsh
	transFun  func(string) string
	tablename string
	byteOrder binary.ByteOrder
}

func NewSearchIndex(ft *fasttext.FastText, dbFilename string) *SearchIndex {
	db, err := sql.Open("sqlite3", dbFilename)
	if err != nil {
		panic(err)
	}
	index := &SearchIndex{
		ft:        ft,
		db:        db,
		lsh:       lsh.NewCosineLsh(300, 16, 10),
		transFun:  func(s string) string { return strings.TrimSpace(strings.ToLower(s)) },
		tablename: TableName,
		byteOrder: ByteOrder,
	}
	if index.checkSqlitTableExist() {
		index.load()
	}
	return index
}

func (index *SearchIndex) Close() error {
	return index.db.Close()
}

func (index *SearchIndex) IsNotBuilt() bool {
	return !index.checkSqlitTableExist()
}

func (index *SearchIndex) Build(ts *wikitable.WikiTableStore) error {
	if index.checkSqlitTableExist() {
		return errors.New("Sqlite database already exists")
	}
	if ts.IsNotBuilt() {
		return errors.New("Wikitable store is not built, build it first")
	}

	// Compute embedding entries from wikitables
	entries := make(chan *EmbEntry)
	go func() {
		defer close(entries)
		ts.Apply(func(table *wikitable.WikiTable) {
			for i, column := range table.Columns {
				if table.Headers[i].IsNum {
					continue
				}
				vec, err := index.GetEmb(column)
				if err != nil {
					continue
				}
				id := toColumnID(table.ID, i)
				index.lsh.Insert(vec, id)
				entry := &EmbEntry{
					TableID:     table.ID,
					ColumnIndex: i,
					Vec:         vec,
				}
				entries <- entry
			}
		})
	}()

	// Saving embedding entries to Sqlite
	_, err := index.db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			table_id TEXT,
			column_index INTEGER,
			vec BLOB
		);
		`, index.tablename))
	if err != nil {
		panic(err)
	}
	stmt, err := index.db.Prepare(fmt.Sprintf(`
		INSERT INTO %s(table_id, column_index, vec) VALUES(?, ?, ?);
		`, index.tablename))
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	for e := range entries {
		binVec := vecToBytes(e.Vec, index.byteOrder)
		if _, err := stmt.Exec(e.TableID, e.ColumnIndex, binVec); err != nil {
			panic(err)
		}
	}
	_, err = index.db.Exec(fmt.Sprintf(`
		CREATE INDEX ind_column_id ON %s(table_id, column_index);
		`, index.tablename))
	if err != nil {
		panic(err)
	}
	return nil
}

// Load recovers the search index entries from the Sqlite database.
func (index *SearchIndex) load() {
	rows, err := index.db.Query(fmt.Sprintf(`
	SELECT table_id, column_index, vec FROM %s;
	`, index.tablename))
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		var tableID string
		var columnIndex int
		var binVec []byte
		if err := rows.Scan(&tableID, &columnIndex, &binVec); err != nil {
			panic(err)
		}
		vec, err := bytesToVec(binVec, index.byteOrder)
		if err != nil {
			panic(err)
		}
		id := toColumnID(tableID, columnIndex)
		index.lsh.Insert(vec, id)
		count++
		fmt.Printf("\rLoaded %d embeddings into index", count)
		if count == 100000 {
			break
		}
	}
	fmt.Println()
}

func (index *SearchIndex) Get(tableID string, columnIndex int) (*EmbEntry, error) {
	var binVec []byte
	err := index.db.QueryRow(fmt.Sprintf(`
	SELECT vec FROM %s
	WHERE table_id=? AND column_index=?;
	`, index.tablename), tableID, columnIndex).Scan(&binVec)
	if err == sql.ErrNoRows {
		return nil, errors.New("The column does not exist in the index")
	}
	if err != nil {
		return nil, err
	}
	vec, err := bytesToVec(binVec, index.byteOrder)
	if err != nil {
		panic(err)
	}
	return &EmbEntry{
		TableID:     tableID,
		ColumnIndex: columnIndex,
		Vec:         vec,
	}, nil
}

func (index *SearchIndex) TopK(query []float64, k int) []*EmbEntry {
	lshResults := index.lsh.Query(query)
	log.Printf("LSH Returns %d candidates", len(lshResults))
	queue := NewTopKQueue(k)
	for _, id := range lshResults {
		tableID, columnIndex := fromColumnID(id)
		entry, err := index.Get(tableID, columnIndex)
		if err != nil {
			panic(err)
		}
		queue.Push(entry, dotProduct(query, entry.Vec))
	}
	result := make([]*EmbEntry, queue.Size())
	for i := len(result) - 1; i >= 0; i-- {
		v, _ := queue.Pop()
		result[i] = v.(*EmbEntry)
	}
	return result
}

func (index *SearchIndex) GetEmb(column []string) ([]float64, error) {
	domain := mkDomain(column, index.transFun)
	var vec []float64
	var count int
	for w := range domain {
		wordparts := strings.Split(w, " ")
		if len(wordparts) > 5 {
			continue
		}
		for _, p := range wordparts {
			emb, err := index.ft.GetEmb(p)
			if err == fasttext.ErrNoEmbFound {
				// log.Printf("No embedding found for %s", p)
				continue
			}
			if err != nil {
				panic(err)
			}
			if vec == nil {
				vec = emb.Vec
			} else {
				add(vec, emb.Vec)
			}
		}
		if vec != nil {
			count++
		}
	}
	if vec == nil {
		return nil, ErrNoEmbFound
	}
	for i, v := range vec {
		vec[i] = v / float64(count)
	}
	return vec, nil
}

func (index *SearchIndex) checkSqlitTableExist() bool {
	var name string
	err := index.db.QueryRow(`
	SELECT name FROM sqlite_master WHERE type='table' AND name=?;
	`, index.tablename).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		panic(err)
	}
	return true
}

func mkDomain(values []string, transFun func(string) string) map[string]bool {
	domain := make(map[string]bool)
	for _, v := range values {
		v = transFun(v)
		domain[v] = true
	}
	return domain
}

func dotProduct(x, y []float64) float64 {
	if len(x) != len(y) {
		panic("Length of vectors not equal")
	}
	p := 0.0
	for i := range x {
		p += x[i] * y[i]
	}
	return p
}

func add(dst, src []float64) {
	if len(dst) != len(src) {
		panic("Length of vectors not equal")
	}
	for i := range src {
		dst[i] = dst[i] + src[i]
	}
}

func vecToBytes(vec []float64, order binary.ByteOrder) []byte {
	buf := new(bytes.Buffer)
	for _, v := range vec {
		binary.Write(buf, order, v)
	}
	return buf.Bytes()
}

func bytesToVec(data []byte, order binary.ByteOrder) ([]float64, error) {
	size := len(data) / 8
	vec := make([]float64, size)
	buf := bytes.NewReader(data)
	var v float64
	for i := range vec {
		if err := binary.Read(buf, order, &v); err != nil {
			return nil, err
		}
		vec[i] = v
	}
	return vec, nil
}

func fromColumnID(columnID string) (tableID string, columnIndex int) {
	items := strings.Split(columnID, ":")
	if len(items) != 2 {
		msg := fmt.Sprintf("Malformed Column ID: %s", columnID)
		panic(msg)
	}
	tableID = items[0]
	var err error
	columnIndex, err = strconv.Atoi(items[1])
	if err != nil {
		msg := fmt.Sprintf("Malformed Column ID: %s", columnID)
		panic(msg)
	}
	return
}

func toColumnID(tableID string, columnIndex int) (columnID string) {
	columnID = fmt.Sprintf("%s:%d", tableID, columnIndex)
	return
}
