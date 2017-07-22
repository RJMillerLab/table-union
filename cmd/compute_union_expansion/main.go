package main

import (
	"github.com/RJMillerLab/table-union/experiment"
)

func main() {
	experiment.CheckEnv()
	//experiment.DoComputeAndSaveExpansion()
	experiment.DoComputeAndSaveRowExpansion()
}
