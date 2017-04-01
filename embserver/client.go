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

func (c *Client) findTopKWords(words []string, vec []float64, k int) []string {
	queue := NewTopKQueue(k)
	for _, v := range words {
		wordVec, err := c.ft.GetEmb(v)
		if err == fasttext.ErrNoEmbFound {
			continue
		}
		if err != nil {
			panic(err)
		}
		queue.Push(v, dotProduct(wordVec.Vec, vec))
	}
	relevantWords := make([]string, queue.Size())
	for x := len(relevantWords) - 1; x >= 0; x-- {
		v, _ := queue.Pop()
		relevantWords[x] = v.(string)
	}
	return relevantWords
}

func (c *Client) Query(queryCSVFilename string, k int, resultDir string) {
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
		for rank, entry := range result {
			log.Printf("(Rank %d) Table %s, Column %d", rank, entry.TableID, entry.ColumnIndex)
			t, err := c.ts.GetTable(entry.TableID)
			// Find the top-k values in this column
			topkWords := c.findTopKWords(t.Columns[entry.ColumnIndex], vecs[i], k)
			// Create output directory for this column
			outputDir := filepath.Join(colResultDir,
				fmt.Sprintf("(Rank %d)_%s_c%d_(%s)",
					rank,
					entry.TableID,
					entry.ColumnIndex,
					url.PathEscape(t.Headers[entry.ColumnIndex].Text)))
			if err := os.MkdirAll(outputDir, 0777); err != nil {
				panic(err)
			}
			// Output the top-k values
			topkWordFilename := filepath.Join(outputDir, fmt.Sprintf("top-%d.csv", k))
			f, err := os.Create(topkWordFilename)
			if err != nil {
				panic(err)
			}
			w := csv.NewWriter(f)
			for _, word := range topkWords {
				if err := w.Write([]string{word}); err != nil {
					panic(err)
				}
			}
			w.Flush()
			if err := w.Error(); err != nil {
				panic(err)
			}
			f.Close()
			// Output original table
			filename := filepath.Join(outputDir, "table.csv")
			f, err = os.Create(filename)
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
