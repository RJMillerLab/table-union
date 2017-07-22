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
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/RJMillerLab/table-union/opendata"
	"github.com/RJMillerLab/table-union/yago"
	"github.com/ekzhu/datatable"
)

var (
	yagoDB             = "/home/kenpu/TABLE_UNION_OUTPUT/yago.sqlite3.0"
	wordEntityFilename = "/home/kenpu/TABLE_UNION_OUTPUT/word-entity.txt"
	classFilename      = "/home/fnargesian/TABLE_UNION_OUTPUT/entity-category.txt"
	yagoFilename       = "/home/fnargesian/TABLE_UNION_OUTPUT/yago.sqlite3.0"
	//classFilename = "/home/fnargesian/TABLE_UNION_OUTPUT/entity-class.txt"
)

type OntologyJaccardClient struct {
	host         string
	cli          *http.Client
	transFun     func(string) string
	tokenFun     func(string) []string
	numHash      int
	entityLookup map[string]map[string]bool
	entityCounts map[string]int
	entityClass  map[string][]string
	yago         *yago.Yago
}

func NewOntologyJaccardClient(host string, numHash int) (*OntologyJaccardClient, error) {
	log.Printf("New jaccard client for experiments.")
	//lookup := loadEntityWords(wordEntityFilename)
	//counts := loadEntityWordCount(yagoDB)
	//classes := loadEntityClasses(classFilename)
	//yg := yago.InitYago(yagoFilename)
	return &OntologyJaccardClient{
		host:     host,
		cli:      &http.Client{},
		transFun: DefaultTransFun,
		tokenFun: DefaultTokenFun,
		numHash:  numHash,
		//entityLookup: lookup,
		//entityCounts: counts,
		//entityClass:  classes,
		//	yago:         yg,
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
		//panic(err)
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
	cards := make([]int, 0)
	noOntVecs := make([][]uint64, 0)
	queryTextHeaders := make([]string, 0)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			ontVec, noOntVec, vec, ontCard, noOntCard, card, err := getAttributeOntologyData(queryCSVFilename, i, c.numHash)
			//ontVec, noOntVec, vec, ontCard, noOntCard, card := opendata.GetOntDomain(c.yago.Copy(), col, c.numHash, c.entityClass, c.transFun, c.tokenFun)
			if err == nil {
				vecs = append(vecs, vec)
				queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
				textToAllHeaders[len(queryTextHeaders)-1] = i
				ontVecs = append(ontVecs, ontVec)
				noOntVecs = append(noOntVecs, noOntVec)
				ontCards = append(ontCards, ontCard)
				noOntCards = append(noOntCards, noOntCard)
				cards = append(cards, card)
			}
		}
	}
	if len(vecs) < k {
		log.Printf("The query has too few text columns for %d-unionability.", k)
		k = len(vecs)
	}
	// Query server
	//resp := c.mkReq(OntologyJaccardQueryRequest{Vecs: vecs, OntVecs: ontVecs, K: k, N: maxN, OntCardinality: ontCards, NoOntCardinality: noOntCards})
	//resp := c.mkReq(OntologyJaccardQueryRequest{Vecs: vecs, OntVecs: ontVecs, K: k, N: maxN, OntCardinality: ontCards, NoOntCardinality: noOntCards})
	resp := c.mkReq(OntologyJaccardQueryRequest{Vecs: vecs, OntVecs: ontVecs, K: k, N: maxN, OntCardinality: ontCards, NoOntCardinality: noOntCards})
	// Process results
	if resp.Result == nil || len(resp.Result) == 0 {
		log.Printf("No result found.")
	}
	for _, result := range resp.Result {
		log.Printf("result in client")
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
		log.Printf("Query %s not found.", queryCSVFilename)
		//panic(err)
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
	noOntVecs := make([][]uint64, 0)
	noOntCards := make([]int, 0)
	cards := make([]int, 0)
	queryTextHeaders := make([]string, 0)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			//ontVec, noOntVec, vec, ontCard, noOntCard, card := opendata.GetOntDomain(c.yago, col, c.numHash, c.entityClass, c.transFun, c.tokenFun)
			ontVec, noOntVec, vec, ontCard, noOntCard, card, err := getAttributeOntologyData(queryCSVFilename, i, c.numHash)
			if err == nil {
				noOntVecs = append(noOntVecs, noOntVec)
				vecs = append(vecs, vec)
				queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
				textToAllHeaders[len(queryTextHeaders)-1] = i
				ontVecs = append(ontVecs, ontVec)
				ontCards = append(ontCards, ontCard)
				noOntCards = append(noOntCards, noOntCard)
				cards = append(cards, card)
			}
		}
	}
	if len(vecs) < minK {
		log.Printf("The query has too few text columns for %d-unionability.", minK)
		minK = len(vecs)
	}
	// Query server
	//mK := int(math.Min(float64(maxK), float64(len(vecs))))
	//for kp := minK; kp < mK; kp++ {
	//for kp := minK; kp < len(vecs); kp++ {
	for _, kp := range ks {
		resp := c.mkReq(OntologyJaccardQueryRequest{Vecs: vecs, OntVecs: ontVecs, NoOntVecs: noOntVecs, K: kp, N: n, Cardinality: cards, OntCardinality: ontCards, NoOntCardinality: noOntCards})
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
			log.Printf("LoadEntityWordCount: %d\n", i)
		}
	}
	return counts
}

func loadEntityClasses(classFilename string) map[string][]string {
	lookup := make(map[string][]string)
	f, err := os.Open(path.Join(classFilename))
	//f, err := os.Open(path.Join(OutputDir, "entity-class.txt"))
	if err != nil {
		panic(err)
	}
	//defer f.Close()
	scanner := bufio.NewScanner(f)
	i := 0
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "|", 2)
		entity, class := strings.ToLower(parts[0]), parts[1]
		if _, ok := lookup[entity]; !ok {
			lookup[entity] = make([]string, 0)
		}
		lookup[entity] = append(lookup[entity], class)
		i += 1
		if i%100000 == 0 {
			fmt.Printf("LoadEntityClasses: %d.\n", i)
		}
	}
	log.Printf("number of entities: %d", len(lookup))
	f.Close()
	return lookup
}

func getAttributeOntologyData(tableID string, colIndex, numHash int) ([]uint64, []uint64, []uint64, int, int, int, error) {
	tableID = strings.Replace(tableID, opendataDir, "", -1)
	ontVecFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ont-minhash-l1", tableID, colIndex))
	ontVec, err := opendata.ReadMinhashSignature(ontVecFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", ontVecFilename)
		//ontVec = make([]uint64, 0)
		return nil, nil, nil, 0, 0, 0, err
	}
	//
	noOntVecFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.noann-minhash", tableID, colIndex))
	noOntVec, err := opendata.ReadMinhashSignature(noOntVecFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", noOntVecFilename)
		//noOntVec = make([]uint64, 0)
		return nil, nil, nil, 0, 0, 0, err
	}
	//
	vecFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.minhash", tableID, colIndex))
	vec, err := opendata.ReadMinhashSignature(vecFilename, numHash)
	if err != nil {
		log.Printf("Error in reading %s from disk.", vecFilename)
		//vec = make([]uint64, 0)
		return nil, nil, nil, 0, 0, 0, err
	}
	//
	cardpath := path.Join(domainDir, tableID, fmt.Sprintf("%d.%s", colIndex, "ont-noann-card"))
	f, err := os.Open(cardpath)
	defer f.Close()
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	noOntCard := 0
	card := 0
	scanner := bufio.NewScanner(f)
	lineIndex := 0
	for scanner.Scan() {
		if lineIndex == 0 {
			c, err := strconv.Atoi(strings.Replace(scanner.Text(), "\n", "", -1))
			if err == nil {
				noOntCard = c
			}
		}
		if lineIndex == 2 {
			c, err := strconv.Atoi(strings.Replace(scanner.Text(), "\n", "", -1))
			if err == nil {
				card = c
			}
		}
		lineIndex += 1
	}
	//
	cardpath = path.Join(domainDir, tableID, fmt.Sprintf("%d.%s", colIndex, "ont-card"))
	f, err = os.Open(cardpath)
	defer f.Close()
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	ontCard := 0
	scanner = bufio.NewScanner(f)
	lineIndex = 0
	for scanner.Scan() {
		if lineIndex == 0 {
			c, err := strconv.Atoi(strings.Replace(scanner.Text(), "\n", "", -1))
			if err == nil {
				ontCard = c
			}
		}
		lineIndex += 1
	}
	//
	return ontVec, noOntVec, vec, ontCard, noOntCard, card, nil
}
