package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/RJMillerLab/fastTextHomeWork/search"
	"github.com/RJMillerLab/fastTextHomeWork/util"
	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
)

func readEmbeddings(embsFile string) (map[string][]float64, int) {
	wembs := make(map[string][]float64)
	lines, _ := util.ReadLines(embsFile)
	var embsize int
	for linx, line := range lines {
		if linx == 0 {
			embsize, _ = strconv.Atoi(strings.Split(strings.TrimSpace(strings.ToLower(line)), " ")[1])
			continue
		}
		lineparts := strings.Split(strings.TrimSpace(strings.ToLower(line)), " ")
		var word string
		embs := make([]float64, 0, embsize)
		embsize = len(lineparts) - 1
		for i, p := range lineparts {
			if i == 0 {
				word = p
			} else {
				sf, _ := strconv.ParseFloat(p, 64)
				embs = append(embs, sf)
			}
		}
		wembs[word] = embs
	}
	return wembs, embsize
}

func mkColumnEmbedding(column []string, wembs map[string][]float64, embSize int) []float64 {
	domain := util.MkDomain(column)
	colEmbs := make([]float64, embSize)
	for _, w := range domain {
		w = strings.TrimSpace(strings.ToLower(w))
		wordparts := strings.Split(w, " ")
		for _, p := range wordparts {
			if util.IsNumeric(p) {
				continue
			}
			emb := wembs[p]
			for i, v := range emb {
				colEmbs[i] += v
			}
		}
	}
	//log.Printf("embs is %v\n", colEmbs)
	return colEmbs
}

func main() {
	var wordembsFile string
	var wtembsFile string
	var wikitablesFile string
	flag.StringVar(&wordembsFile, "wembeddingsfile", "/home/ekzhu/FB_WORD_VEC/wiki.en.vec", "word embeddings file")
	flag.StringVar(&wtembsFile, "wtembsFile", "wikitables_embeddings", "wiki tables embedding json file")
	flag.StringVar(&wikitablesFile, "wikitablesFile", "/home/ekzhu/go/src/github.com/RJMillerLab/fastTextHomeWork/wikitable/testdata/tables.json", "wikitablesFile")
	//
	wembs, embsize := readEmbeddings(wordembsFile)
	wtf, err := os.Open(wikitablesFile)
	if err != nil {
		panic(err)
	}
	defer wtf.Close()
	emf, err := os.Create(wtembsFile)
	if err != nil {
		panic(err)
	}
	wemf := bufio.NewWriter(emf)
	defer emf.Close()
	var entries []*search.VecEntry
	var tables []*wikitable.WikiTable
	for table := range wikitable.ReadWikiTable(wtf) {
		for cid, col := range table.Columns {
			colemb := mkColumnEmbedding(col, wembs, embsize)
			wcemb := &search.VecEntry{
				TableID:     table.ID,
				ColumnIndex: cid,
				Embedding:   colemb,
			}
			wcembbytes, _ := json.Marshal(wcemb)
			if err != nil {
				panic(err)
			}
			wemf.Write(wcembbytes)
			wemf.WriteString("\n")
			wemf.Flush()
			entries = append(entries, wcemb)
			tables = append(tables, table)
		}
	}
	searchindex := &search.SearchIndex{
		Entries: entries,
		Tables:  tables,
	}
	query := entries[2].Embedding
	log.Printf("query1: %v\n", tables[0].Columns[2])
	log.Printf("------------------\n")
	_, cols := searchindex.TopK(query, 5)
	log.Printf("results columns of query 1: %v\n", cols)
	log.Printf("------------------\n")
	qstr := []string{"ottawa", "toronto", "montreal"}
	query = mkColumnEmbedding(qstr, wembs, embsize)
	log.Printf("query2: %v\n", qstr)
	log.Printf("------------------\n")
	_, cols = searchindex.TopK(query, 5)
	log.Printf("results columns of query 2: %v\n", cols)
}
