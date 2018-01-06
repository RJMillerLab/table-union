package main

import (
	"log"

	. "github.com/RJMillerLab/table-union/opendata"
)

func main() {
	CheckEnv()
	ComputeTableStitchingUnionability()
	log.Printf("Done generating alignemnts for stitching correspondences.")
}
