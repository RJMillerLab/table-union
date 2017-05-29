package main

import (
	"flag"

	"github.com/RJMillerLab/table-union/embserver"
)

var (
	FastTextDim = 300
)

func main() {
	var domainDir string
	var port string
	var l, m int
	flag.StringVar(&domainDir, "domain-dir", "/home/fnargesian/TABLE_UNION_OUTPUT/domains",
		"The top-level director for all domain and embedding files")
	flag.StringVar(&port, "port", "4003", "Server port")
	flag.IntVar(&l, "l", 5, "LSH Parameter: number of bands or hash tables")
	flag.IntVar(&m, "m", 20, "LSH Parameter: size of each band or hash key")
	flag.Parse()
	// Build Search Index
	si := embserver.NewSearchIndex(domainDir, embserver.NewCosineLsh(FastTextDim, l, m))
	if err := si.Build(); err != nil {
		panic(err)
	}
	// Start server
	s := embserver.NewServer(si)
	defer s.Close()
	s.Run(port)
}
