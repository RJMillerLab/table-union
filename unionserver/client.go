package unionserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

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
		panic(err)
	}
	return queryResponse
}

func (c *Client) QueryAndPreviewResults(queryCSVFilename string, k, n int) []QueryResult {
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
			}
		}
	}
	if len(vecs) < k {
		log.Printf("The query has too few text columns for %d-unionability.", k)
		return results
	}
	// Query server
	resp := c.mkReq(QueryRequest{Vecs: vecs, K: k, N: n})
	// Output results
	if resp.Result == nil || len(resp.Result) == 0 {
		log.Printf("No result found.")
	}
	for _, cand := range resp.Result {
		log.Printf("---------------------")
		log.Printf("Candidate table: %s", cand.TableUnion.CandTableID)
		log.Printf("%d-unionability socre is: %f", len(cand.TableUnion.Alignment), cand.TableUnion.Kunioability)
		selfUnion := true
		for _, alignedPair := range cand.TableUnion.Alignment {
			d := alignedPair.CandColIndex
			s := alignedPair.QueryColIndex
			score := alignedPair.Sim
			if strings.ToLower(queryTextHeaders[s]) != strings.ToLower(cand.TableUnion.CandHeader[d]) {
				selfUnion = false
			}
			log.Printf("%s -> %s: %f", queryTextHeaders[s], cand.TableUnion.CandHeader[d], score)
			values, err := getDomainValues(domainDir, cand.TableUnion.CandTableID, d)
			if err != nil {
				panic(err)
			}
			jacc := jaccard(queryTable.GetColumn(index(queryHeaders, queryTextHeaders[s])), values)
			log.Printf("jaccard: %f", jacc)
		}
		if selfUnion == true {
			log.Printf("SELF UNION")
		}
		results = append(results, cand)
	}
	return results
}

func (c *Client) Query(queryCSVFilename string, k, n int) []QueryResult {
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
			}
		}
	}
	if len(vecs) < k {
		log.Printf("The query has too few text columns for %d-unionability.", k)
		k = len(vecs)
	}
	// Query server
	resp := c.mkReq(QueryRequest{Vecs: vecs, K: k, N: n})
	// Process results
	if resp.Result == nil || len(resp.Result) == 0 {
		log.Printf("No result found.")
	}
	for _, result := range resp.Result {
		results = append(results, result)
	}
	return results
}
