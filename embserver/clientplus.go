package embserver

import (
	"encoding/csv"
	"log"
	"math"
	"os"
	"strings"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/ekzhu/datatable"
	"github.com/ekzhu/minhash-lsh"
)

var (
	seed      = 1
	numHash   = 256
	threshold = 0.6
)

func (c *Client) QueryPlus(queryCSVFilename string, k int) [][]QueryResult {
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
		vecs[i] = vec
	}
	// Query server for semantically similar domains
	topicResults := make([][]QueryResult, len(vecs))
	finalResults := make([][]QueryResult, len(vecs))
	for i := range vecs {
		if vecs[i] == nil {
			continue
		}
		log.Printf("=== Query results for column %d (%s):", i, headers[i])
		resp := c.mkReq(QueryRequest{Vec: vecs[i], K: k})
		if resp.Result == nil || len(resp.Result) == 0 {
			log.Printf("No result for column %d (%s)", i, headers[i])
			continue
		}
		topicResults[i] = resp.Result
		log.Printf("Number of candidates returned by cosine lsh: %d", len(topicResults[i]))
		// find candidates with low expansion scores
		finalResults[i] = findUnionableColumns(resp.Result, queryTable, i, c.domainDir)
		log.Printf("Number of candidates after filtering using minhash lsh: %d", len(finalResults))
		for rank, result := range finalResults[i] {
			//tid, cinx := getTableColumnIndexFromDomainName(result)
			log.Printf("> (%d) Column %d in %s", rank, result.ColumnIndex, result.TableID)
			fvalues, err := getDomainValues(c.domainDir, result.TableID, result.ColumnIndex)
			if err != nil {
				panic(err)
			}
			//entities, err := getDomainEntities(c.domainDir, tid, cinx)
			//if err != nil {
			//	panic(err)
			//}
			jacc := jaccard(queryTable.GetColumn(i), fvalues)
			cont := containment(queryTable.GetColumn(i), fvalues)
			//entityjacc := Jaccard(c.annotator.DoAnnotateDomain(queryTable.GetColumn(i)), entities)
			//entitycont := Containment(c.annotator.DoAnnotateDomain(queryTable.GetColumn(i)), entities)
			log.Printf("Jaccard: %f", jacc)
			log.Printf("Containment: %f", cont)
			//log.Printf("Entity Jaccard: %f", entityjacc)
			//log.Printf("Entity Containment: %f", entitycont)
			log.Printf("==== values ====")
			log.Printf(strings.Join(fvalues[:int(math.Min(10.0, float64(len(fvalues))))], ","))
			log.Printf("================\n")
		}
	}
	return finalResults
}

func findUnionableColumns(candidates []QueryResult, queryTable *datatable.DataTable, columnIndex int, domainDir string) []QueryResult {
	mhlsh := buildJaccardIndex(candidates, domainDir)
	query := queryTable.GetColumn(columnIndex)
	expansionResults := queryJaccardIndex(mhlsh, query)
	finalResults := filterResults(candidates, expansionResults)
	return finalResults
}

func buildJaccardIndex(queryResults []QueryResult, domainDir string) *minhashlsh.MinhashLSH {
	// create minhash lsh index
	minhashLsh := minhashlsh.NewMinhashLSH32(numHash, threshold)
	for _, entry := range queryResults {
		sig, err := readMinhashSignature(domainDir, entry.TableID, entry.ColumnIndex, numHash)
		if err != nil {
			log.Printf("Could not find minhash for column %d of table %s.\n", entry.ColumnIndex, entry.TableID)
		}
		minhashLsh.Add(getDomainName(entry.TableID, entry.ColumnIndex), sig)
	}
	return minhashLsh
}

func queryJaccardIndex(lsh *minhashlsh.MinhashLSH, query []string) []string {
	qmh := getMinhash(query, seed, numHash)
	results := lsh.Query(qmh.Signature())
	return results
}

func filterResults(r1 []QueryResult, r2 []string) []QueryResult {
	results := make([]QueryResult, 0)
	for _, entry := range r1 {
		dname := getDomainName(entry.TableID, entry.ColumnIndex)
		if contains(r2, dname) == false {
			results = append(results, entry)
		}
	}
	return results
}
