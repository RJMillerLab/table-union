package search

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	fasttext "github.com/ekzhu/go-fasttext"
)

const (
	TableName = "search_index"
)

var (
	ErrNoEmbFound           = errors.New("No embedding found")
	ErrIndexNotEmpty        = errors.New("The search index is not empty")
	ErrExistingWikiTableDir = errors.New("The wikitable directory already exists")
	ErrExistingSqliteTable  = errors.New("The Sqlite database table already exists")
	ErrNoTableFound         = errors.New("No such table found")
	ByteOrder               = binary.BigEndian
)

type EmbEntry struct {
	TableID     int       `json:"table_id"`
	ColumnIndex int       `json:"column_index"`
	Vec         []float64 `json:"vec"`
}

type SearchIndex struct {
	ft           *fasttext.FastText
	wikiTableDir string
	db           *sql.DB     // Sqlite store for the embedding entries
	entries      []*EmbEntry // In-memory store for the embedding entries
	transFun     func(string) string
	tablename    string
	byteOrder    binary.ByteOrder
}

func NewSearchIndex(ft *fasttext.FastText, dbFilename, wikiTableDir string) *SearchIndex {
	db, err := sql.Open("sqlite3", dbFilename)
	if err != nil {
		panic(err)
	}
	index := &SearchIndex{
		ft:           ft,
		wikiTableDir: wikiTableDir,
		db:           db,
		entries:      make([]*EmbEntry, 0),
		transFun:     func(s string) string { return strings.TrimSpace(strings.ToLower(s)) },
		tablename:    TableName,
		byteOrder:    ByteOrder,
	}
	if index.checkSqlitTableExist() {
		err := index.load()
		if err != nil {
			panic(err)
		}
	}
	return index
}

func (index *SearchIndex) Close() error {
	return index.db.Close()
}

func (index *SearchIndex) IsNotBuilt() bool {
	if _, err := os.Stat(index.wikiTableDir); err == nil {
		return false
	}
	if index.checkSqlitTableExist() {
		return false
	}
	return true
}

func (index *SearchIndex) Build(wikiTableFile io.Reader, numThread int) error {
	// Checks
	if _, err := os.Stat(index.wikiTableDir); err == nil {
		return ErrExistingWikiTableDir
	}
	if index.checkSqlitTableExist() {
		return ErrExistingSqliteTable
	}

	// Proc embeddings
	entries := make(chan *EmbEntry)
	tables := make(chan *wikitable.WikiTable)
	go func() {
		defer close(entries)
		defer close(tables)
		for table := range wikitable.ReadWikiTable(wikiTableFile) {
			for i, column := range table.Columns {
				if table.Headers[i].IsNum {
					continue
				}
				vec, err := index.GetEmb(column)
				if err == ErrNoEmbFound {
					continue
				}
				entry := &EmbEntry{
					TableID:     table.ID,
					ColumnIndex: i,
					Vec:         vec,
				}
				index.entries = append(index.entries, entry)
				entries <- entry
			}
			tables <- table
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	// Backing up wiki tables
	go func() {
		defer wg.Done()
		if err := os.Mkdir(index.wikiTableDir, 0777); err != nil {
			panic(err)
		}
		for table := range tables {
			p := filepath.Join(index.wikiTableDir, strconv.Itoa(table.ID))
			f, err := os.Create(p)
			if err != nil {
				panic(err)
			}
			table.ToCsv(f)
			f.Close()
		}
	}()

	// Saving embeddings to Sqlite
	go func() {
		defer wg.Done()
		_, err := index.db.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			table_id INTEGER,
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
	}()

	wg.Wait()
	return nil
}

// Load recovers the search index entries from the Sqlite database.
func (index *SearchIndex) load() error {
	if len(index.entries) > 0 {
		return ErrIndexNotEmpty
	}
	rows, err := index.db.Query(fmt.Sprintf(`
	SELECT table_id, column_index, vec FROM %s;
	`, index.tablename))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var tableID, columnIndex int
		var binVec []byte
		if err := rows.Scan(&tableID, &columnIndex, &binVec); err != nil {
			return err
		}
		vec, err := bytesToVec(binVec, index.byteOrder)
		if err != nil {
			return err
		}
		p := filepath.Join(index.wikiTableDir, strconv.Itoa(tableID))
		if _, err := os.Stat(p); err != nil {
			return ErrNoTableFound
		}
		index.entries = append(index.entries, &EmbEntry{
			TableID:     tableID,
			ColumnIndex: columnIndex,
			Vec:         vec,
		})
	}
	return nil
}

func (index *SearchIndex) GetTable(id int) (*wikitable.WikiTable, error) {
	p := filepath.Join(index.wikiTableDir, strconv.Itoa(id))
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, ErrNoTableFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	t, err := wikitable.FromCsv(f)
	if err != nil {
		return nil, err
	}
	t.ID = id
	return t, nil
}

func (index *SearchIndex) TopK(query []float64, k int) []*EmbEntry {
	queue := NewTopKQueue(k)
	for _, entry := range index.entries {
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
	for w := range domain {
		wordparts := strings.Split(w, " ")
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
	}
	if vec == nil {
		return nil, ErrNoEmbFound
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
