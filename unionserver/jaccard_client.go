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

	"github.com/RJMillerLab/table-union/opendata"
	"github.com/ekzhu/datatable"
)

type JaccardClient struct {
	host     string
	cli      *http.Client
	transFun func(string) string
	tokenFun func(string) []string
	numHash  int
}

func NewJaccardClient(host string, numHash int) (*JaccardClient, error) {
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
		panic(err)
	}
	return queryResponse
}

func (c *JaccardClient) QueryAndPreviewResults(queryCSVFilename string, k, n int) []QueryResult {
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
	vecs := make([][]uint64, 0)
	queryTextHeaders := make([]string, 0)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			vec := opendata.GetDomainMinhash(c.tokenFun, c.transFun, col, c.numHash)
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
	resp := c.mkReq(JaccardQueryRequest{Vecs: vecs, K: k, N: n})
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
		}
		if selfUnion == true {
			log.Printf("SELF UNION")
		}
		results = append(results, cand)
	}
	return results
}

func (c *JaccardClient) Query(queryCSVFilename string, k, n int) []QueryResult {
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
	vecs := make([][]uint64, 0)
	queryTextHeaders := make([]string, 0)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			vec := opendata.GetDomainMinhash(c.tokenFun, c.transFun, col, c.numHash)
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
	resp := c.mkReq(JaccardQueryRequest{Vecs: vecs, K: k, N: n})
	// Output results
	if resp.Result == nil || len(resp.Result) == 0 {
		log.Printf("No result found.")
	}
	for _, cand := range resp.Result {
		results = append(results, cand)
	}
	return results
}
