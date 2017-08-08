package benchmarkserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

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
