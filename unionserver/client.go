package unionserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/ekzhu/datatable"
	fasttext "github.com/ekzhu/go-fasttext"
)

type Client struct {
	ft        *fasttext.FastText
	host      string
	cli       *http.Client
	transFun  func(string) string
	tokenFun  func(string) []string
	domainDir string
}

func NewClient(ft *fasttext.FastText, host, domainDir string) (*Client, error) {
	return &Client{
		ft:        ft,
		host:      host,
		cli:       &http.Client{},
		transFun:  DefaultTransFun,
		tokenFun:  DefaultTokenFun,
		domainDir: domainDir,
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

func (c *Client) Query(queryCSVFilename string, k, n int) []QueryResult {
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	queryTable, err := datatable.FromCSV(reader)
	if err != nil {
		panic(err)
	}
	// Create embeddings
	vecs := make([][]float64, queryTable.NumCol())
	for i := range vecs {
		vec, err := embedding.GetDomainEmbSum(c.ft, c.tokenFun, c.transFun, queryTable.GetColumn(i))
		if err != nil {
			log.Printf("Embedding not found for column %d", i)
			continue
		}
		vecs[i] = vec
	}
	// Query server
	results := make([]QueryResult, 0)
	resp := c.mkReq(QueryRequest{Vecs: vecs, K: k, N: n})
	if resp.Result == nil || len(resp.Result) == 0 {
		log.Printf("No result found.")
	}
	for _, cand := range resp.Result {
		results = append(results, cand)
		log.Printf("%s", cand.TableID)
	}
	return results
}
