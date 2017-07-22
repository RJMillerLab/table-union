package main

import (
	"flag"

	"github.com/RJMillerLab/table-union/benchmarkserver"
	minhashlsh "github.com/RJMillerLab/table-union/minhashlsh"
)

func main() {
	var domainDir string
	var port string
	var threshold float64
	var numHash int
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v3/domains",
		"The top-level director for all domain and embedding files")
	flag.StringVar(&port, "port", "4049", "Server port")
	flag.IntVar(&numHash, "h", 256, "LSH Parameter: number of hash functions")
	flag.Float64Var(&threshold, "t", 0.7, "Search Parameter: k-unionability threshold")
	flag.Parse()
	// Build Search Index
	ui := benchmarkserver.NewJaccardUnionIndex(domainDir, minhashlsh.NewMinhashLSH32(numHash, threshold), numHash)
	if err := ui.NoOntBuild(); err != nil {
		panic(err)
	}
	oi := benchmarkserver.NewJaccardUnionIndex(domainDir, minhashlsh.NewMinhashLSH32(numHash, threshold), numHash)
	if err := oi.OntBuild(); err != nil {
		panic(err)
	}
	// Start server
	s := benchmarkserver.NewOntologyJaccardServer(ui, oi)
	defer s.Close()
	s.Run(port)
}
