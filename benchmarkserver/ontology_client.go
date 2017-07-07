package benchmarkserver

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/RJMillerLab/table-union/opendata"
	"github.com/ekzhu/datatable"
)

var (
	yagoDB             = "/home/kenpu/TABLE_UNION_OUTPUT/yago.sqlite3.0"
	wordEntityFilename = "/home/kenpu/TABLE_UNION_OUTPUT/word-entity.txt"
)

type OntologyJaccardClient struct {
	host         string
	cli          *http.Client
	transFun     func(string) string
	tokenFun     func(string) []string
	numHash      int
	entityLookup map[string]map[string]bool
	entityCounts map[string]int
}

func NewOntologyJaccardClient(host string, numHash int) (*OntologyJaccardClient, error) {
	log.Printf("New jaccard client for experiments.")
	lookup := loadEntityWords(wordEntityFilename)
	counts := loadEntityWordCount(yagoDB)
	return &OntologyJaccardClient{
		host:         host,
		cli:          &http.Client{},
		transFun:     DefaultTransFun,
		tokenFun:     DefaultTokenFun,
		numHash:      numHash,
		entityLookup: lookup,
		entityCounts: counts,
	}, nil
}

func (c *OntologyJaccardClient) mkReq(queryRequest OntologyJaccardQueryRequest) QueryResponse {
	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(&queryRequest); err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", c.host+"/query", buf)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "/application/json")
	resp, err := c.cli.Do(req)
	if err != nil {
		panic(err)
	}
	queryResponseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var queryResponse QueryResponse
	if err := json.Unmarshal(queryResponseData, &queryResponse); err != nil {
		log.Printf("No response found")
		panic(err)
	}
	return queryResponse
}

func (c *OntologyJaccardClient) QueryWithFixedK(queryCSVFilename string, k, maxN int) []QueryResult {
	results := make([]QueryResult, 0)
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	queryTable, err := datatable.FromCSV(reader)
	queryHeaders := queryTable.GetRow(0)
	if err != nil {
		panic(err)
	}
	// Create minhash
	textToAllHeaders := make(map[int]int)
	vecs := make([][]uint64, 0)
	ontVecs := make([][]uint64, 0)
	ontCards := make([]int, 0)
	noOntCards := make([]int, 0)
	queryTextHeaders := make([]string, 0)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			ontVec, vec, ontCard, noOntCard, _ := opendata.GetOntDomain(col, c.numHash, c.entityLookup, c.entityCounts, c.transFun, c.tokenFun)
			if len(vec) != 0 && len(ontVec) != 0 {
				vecs = append(vecs, vec)
				queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
				textToAllHeaders[len(queryTextHeaders)-1] = i
				ontVecs = append(ontVecs, ontVec)
				ontCards = append(ontCards, ontCard)
				noOntCards = append(noOntCards, noOntCard)
			}
		}
	}
	if len(vecs) < k {
		log.Printf("The query has too few text columns for %d-unionability.", k)
		k = len(vecs)
	}
	// Query server
	resp := c.mkReq(OntologyJaccardQueryRequest{Vecs: vecs, OntVecs: ontVecs, K: k, N: maxN, OntCardinality: ontCards, NoOntCardinality: noOntCards})
	// Process results
	if resp.Result == nil || len(resp.Result) == 0 {
		log.Printf("No result found.")
	}
	for _, result := range resp.Result {
		result.TableUnion.QueryHeader = queryHeaders
		result.TableUnion.QueryTextHeader = queryTextHeaders
		// Retrive header index
		for i, pair := range result.TableUnion.Alignment {
			pair.QueryColIndex = textToAllHeaders[pair.QueryColIndex]
			result.TableUnion.Alignment[i] = pair
		}
		results = append(results, result)
	}
	return results
}

func (c *OntologyJaccardClient) QueryWithFixedN(queryCSVFilename string, minK, n int) []QueryResult {
	results := make([]QueryResult, 0)
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	queryTable, err := datatable.FromCSV(reader)
	queryHeaders := queryTable.GetRow(0)
	if err != nil {
		panic(err)
	}
	// Create minhash
	textToAllHeaders := make(map[int]int)
	vecs := make([][]uint64, 0)
	ontVecs := make([][]uint64, 0)
	ontCards := make([]int, 0)
	noOntCards := make([]int, 0)
	queryTextHeaders := make([]string, 0)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			ontVec, vec, ontCard, noOntCard, _ := opendata.GetOntDomain(col, c.numHash, c.entityLookup, c.entityCounts, c.transFun, c.tokenFun)
			if len(vec) != 0 && len(ontVec) != 0 {
				vecs = append(vecs, vec)
				queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
				textToAllHeaders[len(queryTextHeaders)-1] = i
				ontVecs = append(ontVecs, ontVec)
				ontCards = append(ontCards, ontCard)
				noOntCards = append(noOntCards, noOntCard)
			}
		}
	}
	if len(vecs) < minK {
		log.Printf("The query has too few text columns for %d-unionability.", minK)
		minK = len(vecs)
	}
	// Query server
	for kp := minK; kp < len(vecs); kp++ {
		resp := c.mkReq(OntologyJaccardQueryRequest{Vecs: vecs, OntVecs: ontVecs, K: kp, N: n, OntCardinality: ontCards, NoOntCardinality: noOntCards})
		// Process results
		if resp.Result == nil || len(resp.Result) == 0 {
			log.Printf("No result found.")
		}
		for _, result := range resp.Result {
			result.TableUnion.QueryHeader = queryHeaders
			result.TableUnion.QueryTextHeader = queryTextHeaders
			// Retrive header index
			for i, pair := range result.TableUnion.Alignment {
				pair.QueryColIndex = textToAllHeaders[pair.QueryColIndex]
				result.TableUnion.Alignment[i] = pair
			}
			results = append(results, result)
		}
	}
	return results
}

func loadEntityWords(wordEntityFilename string) map[string]map[string]bool {
	lookup := make(map[string]map[string]bool)
	f, err := os.Open(wordEntityFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	i := 0

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "|", 2)
		word, entity := parts[0], parts[1]
		if _, ok := lookup[word]; !ok {
			lookup[word] = make(map[string]bool)
		}
		lookup[word][entity] = true
		i += 1
		if i%100000 == 0 {
			log.Printf("LoadEntityWords: %d.", i)
		}
	}
	return lookup
}

func loadEntityWordCount(yagoDB string) map[string]int {
	counts := make(map[string]int)
	db, err := sql.Open("sqlite3", yagoDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(`select entity, words_count from words_count`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var ent string
		var count int
		if err = rows.Scan(&ent, &count); err != nil {
			panic(err)
		}
		counts[ent] = count
		i += 1
		if i%100000 == 0 {
			fmt.Printf("LoadEntityWordCount: %d\n", i)
		}
	}
	return counts
}
