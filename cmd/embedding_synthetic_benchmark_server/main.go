package main

import (
	"flag"

	"github.com/RJMillerLab/table-union/benchmarkserver"
	"github.com/RJMillerLab/table-union/simhashlsh"
)

var (
	FastTextDim = 300
)

func main() {
	var domainDir string
	var port string
	var threshold float64
	var numHash int
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v3/domains",
		"The top-level director for all domain and embedding files")
	flag.StringVar(&port, "port", "4074", "Server port")
	flag.IntVar(&numHash, "h", 256, "LSH Parameter: number of hash functions")
	flag.Float64Var(&threshold, "t", 0.7, "Search Parameter: k-unionability threshold")
	flag.Parse()
	// Build Search Index
	ui := benchmarkserver.NewUnionIndex(domainDir, simhashlsh.NewCosineLSH(FastTextDim, numHash, threshold))
	if err := ui.Build(); err != nil {
		panic(err)
	}
	// Start server
	s := benchmarkserver.NewServer(ui)
	defer s.Close()
	s.Run(port)
}
