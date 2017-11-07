package benchmarkserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/RJMillerLab/table-union/opendata"
	"github.com/ekzhu/datatable"
	fasttext "github.com/ekzhu/go-fasttext"
)

var (
	queryDir = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4/csvfiles/"
)

type CombinedClient struct {
	ft       *fasttext.FastText
	host     string
	cli      *http.Client
	transFun func(string) string
	tokenFun func(string) []string
	numHash  int
}

func NewCombinedClient(ft *fasttext.FastText, host string, numHash int) (*CombinedClient, error) {
	return &CombinedClient{
		ft:       ft,
		host:     host,
		cli:      &http.Client{},
		transFun: DefaultTransFun,
		tokenFun: DefaultTokenFun,
		numHash:  numHash,
	}, nil
}

func (c *CombinedClient) mkReq(queryRequest CombinedQueryRequest) QueryResponse {
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
	}
	return queryResponse
}

func (c *CombinedClient) Query(queryCSVFilename string, n int) []QueryResult {
	queryRawFilename := strings.Replace(queryCSVFilename, queryDir, "", -1)
	results := make([]QueryResult, 0)
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		return results
	}
	defer f.Close()
	reader := csv.NewReader(f)
	queryTable, err := datatable.FromCSV(reader)
	queryHeaders := queryTable.GetRow(0)
	if err != nil {
		return results
	}
	// Create signatures
	setVecs := make([][]uint64, 0)
	ontVecs := make([][]uint64, 0)
	noOntVecs := make([][]uint64, 0)
	nlMeans := make([][]float64, 0)
	nlCovars := make([][]float64, 0)
	nlCards := make([]int, 0)
	setCards := make([]int, 0)
	ontCards := make([]int, 0)
	noOntCards := make([]int, 0)
	queryTextHeaders := make([]string, 0)
	textToAllHeaders := make(map[int]int)
	for i := 0; i < queryTable.NumCol(); i++ {
		col := queryTable.GetColumn(i)
		if classifyValues(col) == "text" {
			nlMean, nlCovar, err1 := getDomainEmbMeanCovar(queryRawFilename, i)
			ontVec, noOntVec, _, ontCard, noOntCard, _, err2 := getAttributeOntologyData(queryRawFilename, i, c.numHash)
			setVec := opendata.GetDomainMinhash(c.tokenFun, c.transFun, col, c.numHash)
			if err1 == nil && len(nlMean) != 0 && len(nlCovar) != 0 && !containsNan(nlCovar) && !containsNan(nlMean) {
				nlMeans = append(nlMeans, nlMean)
				nlCovars = append(nlCovars, nlCovar)
				nlCards = append(nlCards, len(col))
				queryTextHeaders = append(queryTextHeaders, queryHeaders[i])
				textToAllHeaders[len(queryTextHeaders)-1] = i
			}
			if len(setVec) != 0 {
				setVecs = append(setVecs, setVec)
				setCards = append(setCards, getCardinality(col))
			}
			if err2 == nil {
				ontVecs = append(ontVecs, ontVec)
				noOntVecs = append(noOntVecs, noOntVec)
				ontCards = append(ontCards, ontCard)
				noOntCards = append(noOntCards, noOntCard)
			}
		}
	}
	// Query server
	queryTableID := strings.Replace(queryCSVFilename, queryDir, "", -1)
	resp := c.mkReq(CombinedQueryRequest{SetVecs: setVecs, OntVecs: ontVecs, NoOntVecs: noOntVecs, NlMeans: nlMeans, NlCovars: nlCovars, NlCards: nlCards, SetCards: setCards, OntCards: ontCards, NoOntCards: noOntCards, N: n, QueryTableID: queryTableID})
	// Process results
	if resp.Result == nil || len(resp.Result) == 0 {
		log.Printf("No result found for %s.", queryCSVFilename)
	}
	log.Printf("query: %s", queryCSVFilename)
	for _, result := range resp.Result {
		log.Printf("candidate: %s", result.TableUnion.CandTableID)
		result.TableUnion.QueryHeader = queryHeaders
		result.TableUnion.QueryTextHeader = queryTextHeaders
		// Retrive header index
		log.Printf("table unionability scores: %v", result.TableUnion.CUnionabilityScores)
		log.Printf("table unionability percentile: %v", result.TableUnion.CUnionabilityPercentiles)
		log.Printf("maxC: %d - bestC: %d bestPerc: %f", len(result.TableUnion.CUnionabilityPercentiles), result.TableUnion.BestC, result.TableUnion.CUnionabilityPercentiles[result.TableUnion.BestC-1])
		for i, pair := range result.TableUnion.Alignment {
			log.Printf("%s -> %s : score %f - perc: %f", queryHeaders[pair.QueryColIndex], result.TableUnion.CandHeader[pair.CandColIndex], result.TableUnion.CUnionabilityScores[i], result.TableUnion.CUnionabilityPercentiles[i])
			pair.QueryColIndex = textToAllHeaders[pair.QueryColIndex]
			result.TableUnion.Alignment[i] = pair
		}
		log.Printf("--------------------------")
		results = append(results, result)
	}
	return results
}
