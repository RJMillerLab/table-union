package benchmarkserver

import (
	"bufio"
	"bytes"
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
	"github.com/ekzhu/datatable"
)

var (
	ks = []int{4} //2, 4, 6, 8, 10} //}4} //2, 4, 6, 8, 10, 15}
)

type JaccardClient struct {
	host     string
	cli      *http.Client
	transFun func(string) string
	tokenFun func(string) []string
	numHash  int
}

func NewJaccardClient(host string, numHash int) (*JaccardClient, error) {
	log.Printf("New jaccard client for experiments.")
	return &JaccardClient{
		host:     host,
		cli:      &http.Client{},
		transFun: DefaultTransFun,
		tokenFun: DefaultTokenFun,
		numHash:  numHash,
	}, nil
}

func (c *JaccardClient) mkReq(queryRequest JaccardQueryRequest) QueryResponse {
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

func (c *JaccardClient) QueryWithFixedK(queryCSVFilename string, k, maxN int) []QueryResult {
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
	queryTextHeaders := make([]string, 0)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			vec := opendata.GetDomainMinhash(c.tokenFun, c.transFun, col, c.numHash)
			if len(vec) != 0 {
				vecs = append(vecs, vec)
				queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
				textToAllHeaders[len(queryTextHeaders)-1] = i
			}
		}
	}
	if len(vecs) < k {
		log.Printf("The query has too few text columns for %d-unionability.", k)
		k = len(vecs)
	}
	// Query server
	resp := c.mkReq(JaccardQueryRequest{Vecs: vecs, K: k, N: maxN})
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

func (c *JaccardClient) QueryWithFixedN(queryCSVFilename string, minK, n int) []QueryResult {
	results := make([]QueryResult, 0)
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		//panic(err)
		log.Printf("file not found")
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
	cardinality := make([]int, 0)
	queryTextHeaders := make([]string, 0)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			vec := opendata.GetDomainMinhash(c.tokenFun, c.transFun, col, c.numHash)
			if len(vec) != 0 {
				vecs = append(vecs, vec)
				queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
				textToAllHeaders[len(queryTextHeaders)-1] = i
				cardinality = append(cardinality, getCardinality(col))
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
	for _, kp := range ks {
		resp := c.mkReq(JaccardQueryRequest{Vecs: vecs, K: kp, N: n, Cardinality: cardinality})
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

func getAttributeMinhash(tableID string, colIndex, numHash int) ([]uint64, error) {
	tableID = strings.Replace(tableID, opendataDir, "", -1)
	vecFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.minhash", tableID, colIndex))
	vec, err := opendata.ReadMinhashSignature(vecFilename, numHash)
	if err != nil {
		return nil, err
	}
	//
	return vec, err
}
