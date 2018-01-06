package main

import (
	"flag"

	"github.com/RJMillerLab/table-union/benchmarkserver"
	"github.com/RJMillerLab/table-union/minhashlsh"
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
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v7/domains", "The top-level director for all domain and embedding files")
	//flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/domains", "The top-level director for all domain and embedding files")
	flag.StringVar(&port, "port", "4064", "Server port")
	flag.IntVar(&numHash, "h", 256, "LSH Parameter: number of hash functions")
	flag.Float64Var(&threshold, "t", 0.01, "Search Parameter: k-unionability threshold")
	flag.Parse()
	// Build Search Index
	seti := benchmarkserver.NewJaccardUnionIndex(domainDir, minhashlsh.NewMinhashLSH32(numHash, 0.3), numHash)    //0.7
	semi := benchmarkserver.NewJaccardUnionIndex(domainDir, minhashlsh.NewMinhashLSH32(numHash, 0.3), numHash)    // 0.7
	semseti := benchmarkserver.NewJaccardUnionIndex(domainDir, minhashlsh.NewMinhashLSH32(numHash, 0.3), numHash) // 0.7
	nli := benchmarkserver.NewUnionIndex(domainDir, simhashlsh.NewCosineLSH(FastTextDim, numHash, 0.9))
	if err := seti.Build(); err != nil {
		panic(err)
	}
	if err := semi.OntBuild(); err != nil {
		panic(err)
	}
	if err := semseti.NoOntBuild(); err != nil {
		panic(err)
	}
	if err := nli.Build(); err != nil {
		panic(err)
	}
	// Start server
	s := benchmarkserver.NewCombinedServer(seti, semi, semseti, nli)
	defer s.Close()
	s.Run(port)
}
