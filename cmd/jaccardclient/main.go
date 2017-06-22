package main

import (
	"flag"

	"github.com/RJMillerLab/table-union/unionserver"
)

func main() {
	//var domainDir string
	var host string
	var queryCSVFilename string
	var n, k int
	var numHash int
	var resultDir string
	flag.StringVar(&queryCSVFilename, "query", "",
		"Query CSV file")
	flag.StringVar(&resultDir, "result-dir", "",
		"Query result directory")
	flag.StringVar(&host, "host", "http://localhost:4003", "Server host")
	flag.IntVar(&n, "n", 10, "Search Parameter: top (n,k) unionable tables")
	flag.IntVar(&k, "k", 5, "Search Parameter: top (n,k) unionable tables")
	flag.IntVar(&numHash, "numhash", 256, "Number of hash values for minhash signature")
	flag.Parse()

	client, err := unionserver.NewJaccardClient(host, numHash)
	if err != nil {
		panic(err)
	}
	_ = client.QueryAndPreviewResults(queryCSVFilename, k, n)
}
