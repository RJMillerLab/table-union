package main

import (
	"encoding/csv"
	"flag"
	"io"
	"log"
	"os"

	"github.com/RJMillerLab/fastTextHomeWork/search"
	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	"github.com/ekzhu/counter"
	"github.com/ekzhu/datatable"
	fasttext "github.com/ekzhu/go-fasttext"
)

func readCSV(file io.Reader) *datatable.DataTable {
	reader := csv.NewReader(file)
	var table *datatable.DataTable
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if table == nil {
			table = datatable.NewDataTable(len(row))
		}
		if err := table.AppendRow(row); err != nil {
			panic(err)
		}
	}
	return table
}

func query(index *search.SearchIndex, table *datatable.DataTable) {
	for i := 0; i < table.NumCol(); i++ {
		log.Printf("Querying column %d", i)
		domain := counter.NewCounter()
		table.ApplyColumn(func(j int, v string) error {
			domain.Update(v)
			return nil
		}, i)
		values := make([]string, 0, domain.Unique())
		domain.Apply(func(v interface{}) error {
			values = append(values, v.(string))
			return nil
		})
		vec, err := index.GetEmb(values)
		if err != nil {
			log.Print(err)
			continue
		}
		result := index.TopK(vec, 5)
		for _, v := range result {
			log.Printf("Table %d, Column %d", v.TableID, v.ColumnIndex)
		}
	}
}

func main() {
	var fastTextFilename string
	var fastTextSqliteDB string
	var wikiTableFilename string
	var searchIndexSqliteDB string
	var wikiTableDir string
	var rebuildSearchIndex bool
	var queryCSVFilename string
	flag.StringVar(&fastTextFilename, "fasttext-raw", "/home/ekzhu/FB_WORD_VEC/wiki.en.vec",
		"Facebook fastText word vec file")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs, will be created if not exist")
	flag.StringVar(&wikiTableFilename, "wikitable-raw", "/home/ekzhu/WIKI_TABLE/tables.json",
		"WikiTable dataset file")
	flag.StringVar(&searchIndexSqliteDB, "searchindex-db", "/home/ekzhu/WIKI_TABLE/search-index.db",
		"Sqlite database file for search index vecs, will be created if not exist")
	flag.StringVar(&wikiTableDir, "wikitable-dir", "/home/ekzhu/WIKI_TABLE/tables",
		"Directory for storing wikitable CSV files, will be created if not exist")
	flag.BoolVar(&rebuildSearchIndex, "rebuild-searchindex", false,
		"Set to true to rebuild search index from scratch, the existing search index sqlite database will be removed")
	flag.StringVar(&queryCSVFilename, "query", "",
		"Query CSV file")
	flag.Parse()

	// Create Sqlite DB for fastText if not exists
	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		log.Print("Building FastText Sqlite database from scratch")
		f, err := os.Open(fastTextFilename)
		if err != nil {
			panic(err)
		}
		ft := fasttext.NewFastText(fastTextSqliteDB)
		if err := ft.BuildDB(f); err != nil {
			panic(err)
		}
		f.Close()
		ft.Close()
		log.Print("Finished building FastText Sqlite database")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)
	defer ft.Close()

	// Create wikitable store, build if not exists
	ts := wikitable.NewWikiTableStore(wikiTableDir)
	if ts.IsNotBuilt() {
		log.Print("Building wikitable store from scratch")
		f, err := os.Open(wikiTableFilename)
		if err != nil {
			panic(err)
		}
		if err := ts.Build(f); err != nil {
			panic(err)
		}
		f.Close()
		log.Print("Finish building wikitable store")
	}

	if rebuildSearchIndex {
		if err := os.Remove(searchIndexSqliteDB); err != nil {
			panic(err)
		}
	}

	index := search.NewSearchIndex(ft, searchIndexSqliteDB)
	defer index.Close()
	// Build search index
	if index.IsNotBuilt() {
		log.Print("Building search index from scratch")
		if err := index.Build(ts); err != nil {
			panic(err)
		}
		log.Print("Finish building search index")
	}

	// Test
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	q := readCSV(f)
	query(index, q)
}
