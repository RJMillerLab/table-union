package benchmarkserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/ekzhu/datatable"
	fasttext "github.com/ekzhu/go-fasttext"
)

var (
	domainDir = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4/domains"
	//opendataDir = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4/csvfiles"
	//domainDir     = "/home/fnargesian/TABLE_UNION_OUTPUT/domains"
	opendataDir   = "/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only"
	opendataDirUS = "/home/ekzhu/OPENDATA"
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
	var queryResponse QueryResponse
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
		if nErr, ok := err.(*net.OpError); ok {
			if nErr.Err == syscall.EPIPE {
				log.Printf("broken pipe! retrying!")
				resp, err = c.cli.Do(req)
				if err != nil {
					log.Printf("retring failed!")
					return queryResponse
				}
			}
		}
	}
	queryResponseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
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
				//log.Printf("Embedding not found for column %d", i)
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
		//log.Printf("query: %s and candidate: %s", queryCSVFilename, result.TableUnion.CandTableID)
		for i, pair := range result.TableUnion.Alignment {
			//		log.Printf("%s %d %d -> %s %d %d: %f", queryTextHeaders[pair.QueryColIndex], len(queryTextHeaders), pair.QueryColIndex, result.TableUnion.CandTextHeader[pair.CandColIndex], len(result.TableUnion.CandTextHeader), pair.CandColIndex, pair.Sim)
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
		//log.Printf("dataset file not found")
		return results
		//panic(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	queryTable, err := datatable.FromCSV(reader)
	queryHeaders := queryTable.GetRow(0)
	if err != nil {
		//log.Printf("error in reading datasets.")
		return results
		//panic(err)
	}
	// Create embeddings
	means := make([][]float64, 0)
	covars := make([][]float64, 0)
	cards := make([]int, 0)
	queryTextHeaders := make([]string, 0)
	textToAllHeaders := make(map[int]int)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			//mean, covar, err := embedding.GetDomainEmbMeanCovar(c.ft, c.tokenFun, c.transFun, col)
			//mean, err := embedding.GetDomainEmbSum(c.ft, c.tokenFun, c.transFun, col)
			mean, covar, err := getDomainEmbMeanCovar(queryCSVFilename, i)
			if err != nil {
				//log.Printf("Embedding not found for column %d", i)
				continue
			}
			if len(mean) != 0 && len(covar) != 0 && !containsNan(covar) && !containsNan(mean) {
				if len(mean) != 0 && len(covar) == 300 {
					means = append(means, mean)
					covars = append(covars, covar)
					size := len(col)
					cards = append(cards, size)
					//cards = append(cards, getCardinality(col))
					queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
					textToAllHeaders[len(queryTextHeaders)-1] = i
				}
			}
		}
	}
	if len(means) < minK {
		log.Printf("The query has too few text columns for %d-unionability.", minK)
		minK = len(means)
	}
	// Query server
	if len(covars) == 0 {
		return results
	}
	for _, kp := range ks {
		resp := c.mkReq(QueryRequest{Vecs: means, Covars: covars, K: kp, N: n, Cards: cards})
		//resp := c.mkReq(QueryRequest{Vecs: means, K: kp, N: n, Cards: cards})
		//resp := c.mkReq(QueryRequest{Vecs: means, K: kp, N: n})
		// Process results
		if resp.Result == nil || len(resp.Result) == 0 {
			log.Printf("No result found for %s.", queryCSVFilename)
		}
		//log.Printf("query: %s", queryCSVFilename)
		for _, result := range resp.Result {
			//log.Printf("candidate: %s", result.TableUnion.CandTableID)
			result.TableUnion.QueryHeader = queryHeaders
			result.TableUnion.QueryTextHeader = queryTextHeaders
			// Retrive header index
			for i, pair := range result.TableUnion.Alignment {
				//log.Printf("%s -> %s : %f", queryTextHeaders[pair.QueryColIndex], result.TableUnion.CandHeader[pair.CandColIndex], pair.Sim)
				pair.QueryColIndex = textToAllHeaders[pair.QueryColIndex]
				result.TableUnion.Alignment[i] = pair
			}
			//log.Printf("--------------------------")
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

func getDomainEmbMeanCovar(tableID string, colIndex int) ([]float64, []float64, error) {
	if strings.HasPrefix(tableID, "us.") {
		tableID = strings.Replace(tableID, opendataDirUS, "", -1)
	} else {
		tableID = strings.Replace(tableID, opendataDir, "", -1)
	}
	meanFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-mean", tableID, colIndex))
	if _, err := os.Stat(meanFilename); os.IsNotExist(err) {
		//log.Printf("Mean embedding file %s does not exist.", meanFilename)
		return nil, nil, err
	}
	mean, err := embedding.ReadVecFromDisk(meanFilename, ByteOrder)
	if err != nil {
		//log.Printf("Error in reading %s from disk.", meanFilename)
		return nil, nil, err
	}
	// reading covariance matrix
	covarFilename := filepath.Join(domainDir, fmt.Sprintf("%s/%d.ft-covar", tableID, colIndex))
	if _, err := os.Stat(covarFilename); os.IsNotExist(err) {
		//log.Printf("Embedding file %s does not exist.", covarFilename)
		return nil, nil, err
	}
	covar, err := embedding.ReadVecFromDisk(covarFilename, ByteOrder)
	if err != nil {
		//log.Printf("Error in reading %s from disk.", covarFilename)
		return nil, nil, err
	}
	return mean, covar, nil
}

func getNow() float64 {
	return float64(time.Now().UnixNano()) / 1E9
}
