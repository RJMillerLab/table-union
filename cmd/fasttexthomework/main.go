package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/RJMillerLab/fastTextHomeWork/search"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var fastTextFilename string
	var fastTextSqliteDB string
	var wikiTableFilename string
	var searchIndexSqliteDB string
	var wikiTableDir string
	var rebuildSearchIndex bool
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
		"Set to true to rebuild search index from scratch, the original index will be removed")
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

	if rebuildSearchIndex {
		if err := os.RemoveAll(wikiTableDir); err != nil {
			panic(err)
		}
		if err := os.Remove(searchIndexSqliteDB); err != nil {
			panic(err)
		}
	}

	index := search.NewSearchIndex(ft, searchIndexSqliteDB, wikiTableDir)
	defer index.Close()
	// Build search index
	if index.IsNotBuilt() {
		log.Print("Building search index from scratch")
		f, err := os.Open(wikiTableFilename)
		if err != nil {
			panic(err)
		}
		if err := index.Build(f, 8); err != nil {
			panic(err)
		}
		f.Close()
		log.Print("Finish building search index")
	}

	// Test
	// TODO: use a file (e.g., CSV) as input instead
	qstr := []string{"ottawa", "toronto", "montreal"}
	log.Println("Test query:", qstr)
	// Build the embedding vector of the test words
	vec, err := index.GetEmb(qstr)
	if err != nil {
		log.Print(err)
		return
	}
	// query
	result := index.TopK(vec, 5)
	for _, v := range result {
		fmt.Printf("Table %d, Column %d\n", v.TableID, v.ColumnIndex)
		table, err := index.GetTable(v.TableID)
		if err != nil {
			panic(err)
		}
		table.ToCsv(os.Stdout)
	}
}
