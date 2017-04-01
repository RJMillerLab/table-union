package embserver

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/RJMillerLab/fastTextHomeWork/wikitable"
	"github.com/ekzhu/datatable"
	fasttext "github.com/ekzhu/go-fasttext"
)

type Client struct {
	ft       *fasttext.FastText
	ts       *wikitable.WikiTableStore
	host     string
	cli      *http.Client
	transFun func(string) string
}

func NewClient(ft *fasttext.FastText, ts *wikitable.WikiTableStore, host string) (*Client, error) {
	if ts.IsNotBuilt() {
		return nil, errors.New("The WikiTable directory is not built")
	}
	return &Client{
		ft:       ft,
		ts:       ts,
		host:     host,
		cli:      &http.Client{},
		transFun: TransFunc,
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
		panic(err)
	}
	return queryResponse
}

func (c *Client) Query(queryCSVFilename string, k int, resultDir string) {
	f, err := os.Open(queryCSVFilename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	headers, err := reader.Read()
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
		vec, err := getColEmb(c.ft, c.transFun, queryTable.GetColumn(i))
		if err != nil {
			log.Printf("Embedding not found for column %d", i)
			continue
		}
		vecs[i] = vec
	}
	// Query server
	results := make([][]QueryResult, len(vecs))
	for i := range vecs {
		if vecs[i] == nil {
			continue
		}
		log.Printf("Querying with column %d", i)
		resp := c.mkReq(QueryRequest{Vec: vecs[i], K: k})
		results[i] = resp.Result
	}
	// Process results
	if err := os.MkdirAll(resultDir, 0777); err != nil {
		panic(err)
	}
	for i, result := range results {
		if result == nil || len(result) == 0 {
			log.Printf("No result for column %d (%s)", i, headers[i])
			continue
		}
		log.Printf("Result for column %d (%s)", i, headers[i])
		colResultDir := filepath.Join(resultDir, fmt.Sprintf("c%d_(%s)", i, headers[i]))
		if err := os.MkdirAll(colResultDir, 0777); err != nil {
			panic(err)
		}
		for i, v := range result {
			log.Printf("(Rank %d) Table %s, Column %d", i, v.TableID, v.ColumnIndex)
			t, err := c.ts.GetTable(v.TableID)
			outputFilename := filepath.Join(colResultDir,
				fmt.Sprintf("(Rank %d)_%s_c%d_(%s).csv",
					i,
					v.TableID,
					v.ColumnIndex,
					url.PathEscape(t.Headers[v.ColumnIndex].Text)))
			f, err := os.Create(outputFilename)
			if err != nil {
				panic(err)
			}
			if err := t.ToCSV(f); err != nil {
				panic(err)
			}
			f.Close()
		}
	}
}
