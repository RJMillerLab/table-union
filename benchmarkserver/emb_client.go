package benchmarkserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/ekzhu/datatable"
	fasttext "github.com/ekzhu/go-fasttext"
)

var (
	domainDir = "/home/fnargesian/TABLE_UNION_OUTPUT/domains"
)

type Client struct {
	ft       *fasttext.FastText
	host     string
	cli      *http.Client
	transFun func(string) string
	tokenFun func(string) []string
}

func NewClient(ft *fasttext.FastText, host string) (*Client, error) {
	log.Printf("New emb client for experiments.")
	return &Client{
		ft:       ft,
		host:     host,
		cli:      &http.Client{},
		transFun: DefaultTransFun,
		tokenFun: DefaultTokenFun,
	}, nil
}

func (c *Client) mkReq(queryRequest QueryRequest) QueryResponse {
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

func (c *Client) QueryWithFixedK(queryCSVFilename string, k, maxN int) []QueryResult {
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
	// Create embeddings
	vecs := make([][]float64, 0)
	queryTextHeaders := make([]string, 0)
	textToAllHeaders := make(map[int]int)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			vec, err := embedding.GetDomainEmbSum(c.ft, c.tokenFun, c.transFun, col)
			if err != nil {
				log.Printf("Embedding not found for column %d", i)
				continue
			}
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
	resp := c.mkReq(QueryRequest{Vecs: vecs, K: k, N: maxN})
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

func (c *Client) QueryWithFixedN(queryCSVFilename string, minK, n int) []QueryResult {
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
	// Create embeddings
	means := make([][]float64, 0)
	//covars := make([][]float64, 0)
	//cards := make([]int, 0)
	queryTextHeaders := make([]string, 0)
	textToAllHeaders := make(map[int]int)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			//mean, covar, err := embedding.GetDomainEmbMeanCovar(c.ft, c.tokenFun, c.transFun, col)
			mean, err := embedding.GetDomainEmbSum(c.ft, c.tokenFun, c.transFun, col)
			if err != nil {
				log.Printf("Embedding not found for column %d", i)
				continue
			}
			//if len(mean) != 0 && len(covar) != 0 && !containsNan(covar) && !containsNan(mean) {
			if len(mean) != 0 {
				means = append(means, mean)
				//covars = append(covars, covar)
				//cards = append(cards, getCardinality(col))
				queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
				textToAllHeaders[len(queryTextHeaders)-1] = i
			}
		}
	}
	if len(means) < minK {
		log.Printf("The query has too few text columns for %d-unionability.", minK)
		minK = len(means)
	}
	// Query server
	for _, kp := range ks {
		//resp := c.mkReq(QueryRequest{Vecs: means, Covars: covars, K: kp, N: n, Cards: cards})
		resp := c.mkReq(QueryRequest{Vecs: means, K: kp, N: n})
		// Process results
		if resp.Result == nil || len(resp.Result) == 0 {
			log.Printf("No result found for %s.", queryCSVFilename)
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

func containsNan(matrix []float64) bool {
	for _, v := range matrix {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return true
		}
	}
	return false
}
