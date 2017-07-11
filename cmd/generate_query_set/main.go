package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	getStrantifiedSamples(1000)
}
func getRandomSamples() {
	embTables := getEmbTables()
	log.Printf("embTableS: %d", len(embTables))
	queryTables := make([]string, 0)
	eNum := 0
	oNum := 0
	for filename := range StreamFilenames() {
		if len(queryTables) == 3000 {
			break
		}
		textDomains := getTextDomains(filename)
		for _, index := range textDomains {
			filepath := path.Join(OutputDir, "domains", filename, fmt.Sprintf("%d.entities", index))
			_, err := os.Open(filepath)
			if err == nil {
				queryTables = append(queryTables, filename)
				oNum += 1
				break
			}
		}
		//}
	}

	fout, err := os.OpenFile("/home/fnargesian/TABLE_UNION_OUTPUT/stratified.queries.shuffle", os.O_CREATE|os.O_WRONLY, 0644)
	defer fout.Close()

	if err != nil {
		panic(err)
	}
	for _, query := range queryTables {
		fmt.Fprintf(fout, "%s\n", query)
	}
	log.Printf("Number of ontology queries: %d", oNum)
	log.Printf("Number of embedding queries: %d", eNum)
	log.Printf("Total number of queries: %d", len(queryTables))
}

func getStrantifiedSamples(sampleSize int) {
	ontTables := make(map[string]bool)
	embTables := make(map[string]bool)
	valTables := make(map[string]bool)
	ontEmbTables := make(map[string]bool)
	for filename := range StreamFilenames() {
		textDomains := getTextDomains(filename)
		if len(textDomains) < 3 && len(textDomains) > 30 {
			if _, ok := valTables[filename]; !ok {
				valTables[filename] = true
			}
			continue
		}
		for _, index := range textDomains {
			filepath := path.Join(OutputDir, "domains", filename, fmt.Sprintf("%d.ont-minhash-l1", index))
			f, err1 := os.Open(filepath)
			f.Close()
			if err1 == nil {
				filepath := path.Join(OutputDir, "domains", filename, fmt.Sprintf("%d.ft-sum", index))
				f, err := os.Open(filepath)
				f.Close()
				if err == nil {
					if _, ok := ontEmbTables[filename]; !ok {
						ontEmbTables[filename] = true
					}
					continue
				} else {
					if _, ok := ontTables[filename]; !ok {
						ontTables[filename] = true
					}
					continue
				}
			} else {
				filepath := path.Join(OutputDir, "domains", filename, fmt.Sprintf("%d.ft-sum", index))
				f, err := os.Open(filepath)
				f.Close()
				if err == nil {
					embTables[filename] = true
				}
			}
		}
	}
	log.Printf("Num of val tables: %d", len(valTables))
	log.Printf("Num of emb tables: %d", len(embTables))
	log.Printf("Num of ont tables: %d", len(ontTables))
	log.Printf("Num of ont and emb tables: %d", len(ontEmbTables))
	numTables := len(embTables) + len(ontTables) + len(ontEmbTables)
	ontSamples := make([]string, 0)
	embSamples := make([]string, 0)
	ontEmbSamples := make([]string, 0)
	fout, err := os.OpenFile("/home/fnargesian/TABLE_UNION_OUTPUT/stratified.queries.shuffle", os.O_CREATE|os.O_WRONLY, 0644)
	defer fout.Close()
	if err != nil {
		panic(err)
	}
	// shuffle tables
	es := make([]string, 0)
	for t, _ := range embTables {
		es = append(es, t)
	}
	embTablesSh := make([]string, len(es))
	perm := rand.Perm(len(es))
	for i, v := range perm {
		embTablesSh[v] = es[i]
	}
	//
	oes := make([]string, 0)
	for t, _ := range ontEmbTables {
		oes = append(oes, t)
	}
	ontEmbTablesSh := make([]string, len(oes))
	perm = rand.Perm(len(oes))
	for i, v := range perm {
		ontEmbTablesSh[v] = oes[i]
	}
	//
	oos := make([]string, 0)
	for t, _ := range ontTables {
		oos = append(oos, t)
	}
	ontTablesSh := make([]string, len(oos))
	perm = rand.Perm(len(oos))
	for i, v := range perm {
		ontTablesSh[v] = oos[i]
	}
	//
	lineNum := 0
	for _, k := range embTablesSh {
		if len(embSamples) > int((float64(len(embTablesSh)*sampleSize) / float64(numTables))) {
			continue
		}
		if lineNum != sampleSize {
			filepath := path.Join(OpendataDir, k)
			f, err := os.Open(filepath)
			f.Close()
			if err == nil {
				embSamples = append(embSamples, k)
				fmt.Fprintf(fout, "%s\n", k)
				lineNum += 1
			} else {
				log.Printf("%s does not exists.", filepath)
			}
		}
	}
	for _, k := range ontEmbTablesSh {
		if len(ontEmbSamples) > int((float64(len(ontEmbTablesSh)*sampleSize) / float64(numTables))) {
			continue
		}
		if lineNum != sampleSize {
			filepath := path.Join(OpendataDir, k)
			f, err := os.Open(filepath)
			f.Close()
			if err == nil {
				ontEmbSamples = append(ontEmbSamples, k)
				fmt.Fprintf(fout, "%s\n", k)
				lineNum += 1
			} else {
				log.Printf("%s does not exists.", filepath)
			}
		}
	}
	for _, k := range ontTablesSh {
		if len(ontSamples) > int((float64(len(ontTablesSh)*sampleSize) / float64(numTables))) {
			continue
		}
		if lineNum != sampleSize {
			filepath := path.Join(OpendataDir, k)
			f, err := os.Open(filepath)
			f.Close()
			if err == nil {
				ontSamples = append(ontSamples, k)
				fmt.Fprintf(fout, "%s\n", k)
				lineNum += 1
			} else {
				log.Printf("%s does not exists.", filepath)
			}
		}
	}
	//log.Printf("Num of val samples: %d", len(valSamples))
	log.Printf("Num of emb samples: %d", len(embSamples))
	log.Printf("Num of ont samples: %d", len(ontSamples))
	log.Printf("Num of ont and emb samples: %d", len(ontEmbSamples))
}
func getEmbTables() map[string]bool {
	db, err := sql.Open("sqlite3", "/home/fnargesian/TABLE_UNION_OUTPUT/cod_fasttext_matches.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT DISTINCT datafile FROM en_query;`)
	if err != nil {
		panic(err)
	}
	//
	embTables := make(map[string]bool)
	for rows.Next() {
		var datafile string
		err := rows.Scan(&datafile)
		datafile = strings.Replace(datafile, "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only/", "", -1)
		if err != nil {
			panic(err)
		}
		embTables[datafile] = true
	}
	rows.Close()
	return embTables
}

func getTextDomains(file string) (indices []int) {
	typesFile := path.Join(OutputDir, "domains", file, "types")
	f, err := os.Open(typesFile)
	defer f.Close()
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		if len(parts) == 2 {
			index, err := strconv.Atoi(parts[0])
			if err != nil {
				panic(err)
			}
			if parts[1] == "text" {
				indices = append(indices, index)
			}
		}
	}

	return
}
