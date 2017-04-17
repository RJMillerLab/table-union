package main

import (
	"flag"
	"log"
	"os"

	"github.com/RJMillerLab/table-union/embserver"
	"github.com/RJMillerLab/table-union/table"
	fasttext "github.com/ekzhu/go-fasttext"
)

var (
	FastTextDim = 300
)

func main() {
	var fastTextFilename string
	var fastTextSqliteDB string
	var wikiTableFilename string
	var searchIndexSqliteDB string
	var wikiTableDir string
	var ontWikiTableDir string
	var rebuildSearchIndex bool
	var rebuildWikiTableStore bool
	var rebuildOntWikiTableStore bool
	var port string
	var l, m int
	var pcsNum int
	flag.StringVar(&fastTextFilename, "fasttext-raw", "/home/ekzhu/FB_WORD_VEC/wiki.en.vec",
		"Facebook fastText word vec file")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs, will be created if not exist")
	flag.StringVar(&wikiTableFilename, "wikitable-raw", "/home/ekzhu/WIKI_TABLE/tables.json",
		"WikiTable dataset file")
	flag.StringVar(&searchIndexSqliteDB, "searchindex-db", "/home/fnargesian/WIKI_TABLE/search-index.db",
		"sqlite database file for search index vecs, will be created if not exist")
	flag.StringVar(&wikiTableDir, "wikitable-dir", "/home/ekzhu/WIKI_TABLE/tables",
		"Directory for storing wikitable CSV files, will be created if not exist")
	flag.StringVar(&ontWikiTableDir, "ontwikitable-dir", "/home/fnargesian/WIKI_TABLE/onttables",
		"Directory for storing wikitable CSV files using entity labels instead of raw text, will be created if not exist")
	flag.BoolVar(&rebuildWikiTableStore, "rebuild-wikitable", false,
		"Set to true to rebuild wikitable store, existing CSV files will be skipped")
	flag.BoolVar(&rebuildOntWikiTableStore, "rebuild-ontwikitable", false,
		"Set to true to rebuild ontology wikitable store, existing CSV files will be skipped")
	flag.BoolVar(&rebuildSearchIndex, "rebuild-searchindex", false,
		"Set to true to rebuild search index from scratch, the existing search index sqlite database will be removed")
	flag.StringVar(&port, "port", "4003", "Server port")
	flag.IntVar(&l, "l", 5, "LSH Parameter: number of bands or hash tables")
	flag.IntVar(&m, "m", 20, "LSH Parameter: size of each band or hash key")
	flag.IntVar(&pcsNum, "pcs", 3, "Number of principal components for representing a domain")
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
	ts := table.NewTableStore(wikiTableDir)
	if ts.IsNotBuilt() || rebuildWikiTableStore {
		log.Print("Building wikitable store")
		f, err := os.Open(wikiTableFilename)
		if err != nil {
			panic(err)
		}
		if err := ts.BuildWT(f); err != nil {
			panic(err)
		}
		f.Close()
		log.Print("Finish building wikitable store")
	}

	// Create ontology wikitable store, build if not exists
	ots := table.NewTableStore(ontWikiTableDir)
	if ots.IsNotBuilt() || rebuildOntWikiTableStore {
		log.Print("Building ontology wikitable store")
		f, err := os.Open(wikiTableFilename)
		if err != nil {
			panic(err)
		}
		if err := ots.OntBuild(f); err != nil {
			panic(err)
		}
		f.Close()
		log.Print("Finish building ontology wikitable store")
	}
	if rebuildSearchIndex {
		if err := os.Remove(searchIndexSqliteDB); err != nil {
			panic(err)
		}
	}

	si := embserver.NewSearchIndex(ft, searchIndexSqliteDB, embserver.NewCosineLsh(FastTextDim, l, m), pcsNum)
	// Build search index if it is not built
	if si.IsNotBuilt() {
		log.Print("Building search index from scratch")
		if err := si.Build(ts); err != nil {
			panic(err)
		}
		log.Print("Finish building search index")
	} else {
		si.Load()
	}

	// Start server
	s := embserver.NewServer(ft, ts, si)
	defer s.Close()
	s.Run(port)
}
