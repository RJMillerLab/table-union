package embserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strings"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/RJMillerLab/table-union/ontology"
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
	annotator *ontology.Annotator
}

func NewClient(ft *fasttext.FastText, host, domainDir string) (*Client, error) {
	//annotator := ontology.NewAnnotator()
	return &Client{
		ft:        ft,
		host:      host,
		cli:       &http.Client{},
		transFun:  DefaultTransFun,
		tokenFun:  DefaultTokenFun,
		domainDir: domainDir,
		//	annotator: annotator,
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

func (c *Client) Query(queryCSVFilename string, k int) [][]QueryResult {
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	headers, err := reader.Read() // Assume first row is header
	if err != nil {
		panic(err)
	}
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
		//	vecs[i] = pcvecs[0]
		vecs[i] = vec
	}
	// Query server
	results := make([][]QueryResult, len(vecs))
	//values := make([]string, 0)
	for i := range vecs {
		if vecs[i] == nil {
			continue
		}
		log.Printf("=== Querying with column %d", i)
		resp := c.mkReq(QueryRequest{Vec: vecs[i], K: k})
		if resp.Result == nil || len(resp.Result) == 0 {
			log.Printf("No result for column %d (%s)", i, headers[i])
			continue
		}
		results[i] = resp.Result
		log.Printf("Query results for column %d (%s):", i, headers[i])
		for rank, entry := range resp.Result {
			log.Printf("> (%d) Column %d in %s", rank, entry.ColumnIndex, entry.TableID)
			values, err := getDomainValues(c.domainDir, entry.TableID, entry.ColumnIndex)
			if err != nil {
				panic(err)
			}
			entities, err := getDomainEntities(c.domainDir, entry.TableID, entry.ColumnIndex)
			if err != nil {
				panic(err)
			}
			jacc := jaccard(queryTable.GetColumn(i), values)
			cont := containment(queryTable.GetColumn(i), values)
			entityjacc := jaccard(c.annotator.DoAnnotateDomain(queryTable.GetColumn(i)), entities)
			entitycont := containment(c.annotator.DoAnnotateDomain(queryTable.GetColumn(i)), entities)
			log.Printf("Jaccard: %f", jacc)
			log.Printf("Containment: %f", cont)
			log.Printf("Entity Jaccard: %f", entityjacc)
			log.Printf("Entity Containment: %f", entitycont)
			log.Printf("==== values ====")
			log.Printf(strings.Join(values[:int(math.Min(10.0, float64(len(values))))], ","))
			log.Printf("================\n")
		}
	}
	return results
}
