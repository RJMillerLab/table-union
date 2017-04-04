package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/RJMillerLab/fastTextHomeWork/embserver"
	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	fasttext "github.com/ekzhu/go-fasttext"
)

func main() {
	var host string
	var queryCSVFilename string
	var resultDir string
	var k int
	var benchmarkDir string
	var fastTextSqliteDB string
	var batchQueryFilename string
	var groundtruthFile string
	flag.StringVar(&benchmarkDir, "benchmark-dir", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/testdata",
		"Directory where benchmark CSV files are stored.")
	flag.StringVar(&groundtruthFile, "groundtruth", "/home/fnargesian/go/src/github.com/RJMillerLab/fastTextHomeWork/benchmark/groundtruth.json",
		"Ground truth file - based on WWT benchmark.")
	flag.StringVar(&fastTextSqliteDB, "fasttext-db", "/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"Sqlite database file for fastText vecs")
	flag.StringVar(&queryCSVFilename, "query", "",
		"Query CSV file")
	flag.StringVar(&resultDir, "result-dir", "",
		"Query result directory")
	flag.StringVar(&batchQueryFilename, "batch-query", "", "The file containing a list of queries")
	flag.StringVar(&host, "host", "http://localhost:4004", "Server host")
	flag.IntVar(&k, "k", 5, "Top-K")
	flag.Parse()

	if _, err := os.Stat(fastTextSqliteDB); os.IsNotExist(err) {
		panic("FastText Sqlite DB does not exist")
	}
	ft := fasttext.NewFastText(fastTextSqliteDB)
	ts := wikitable.NewWikiTableStore(benchmarkDir)

	client, err := embserver.NewClient(ft, ts, host)
	if err != nil {
		panic(err)
	}
	if batchQueryFilename != "" {
		var tp int
		var total_result int
		// load ground truth
		gt := make(map[string][]string)
		loadJson(groundtruthFile, &gt)
		f, err := os.Open(batchQueryFilename)
		if err != nil {
			fmt.Printf("Query file panic")
			panic(err.Error())
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			queryCSVFilename := scanner.Text()
			qname := queryCSVFilename[strings.LastIndex(queryCSVFilename, "/")+1:]
			client.Query(queryCSVFilename, k, filepath.Join(resultDir, qname))
			rf, err := os.Open(filepath.Join(filepath.Join(resultDir, qname), "results"))
			if err != nil {
				fmt.Printf("result file panic")
				continue
			}
			res_scanner := bufio.NewScanner(rf)
			for res_scanner.Scan() {
				parts := strings.Split(res_scanner.Text(), " ")
				total_result += 1
				if contains(gt[qname], parts[0]) {
					tp += 1
				}
			}
		}
		log.Printf("Precision: %f", float64(tp)/float64(total_result))
	}
	if queryCSVFilename != "" {
		client.Query(queryCSVFilename, k, resultDir)
	}
}

func loadJson(file string, v interface{}) {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err.Error())
	}
	err = json.Unmarshal(buffer, v)
	if err != nil {
		panic(err.Error())
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
