package main

import (
	"github.com/RJMillerLab/table-union/experiment"
)

func main() {
	experiment.CheckEnv()
	experiment.DoComputeAndSaveExpansion()
	/*
		columnPairs := experiment.ReadColumnPairs()
		expansions := experiment.ComputeUnionPairExpansion(columnPairs, 35)
		progress := experiment.SaveExpansion(expansions)
		i := 0
		total := ProgressCounter{}
		start := GetNow()
		tick := GetNow()
		for n := range progress {
			total.Values += n.Values
			i += 1
			now := GetNow()

			if now-tick > 10 {
				tick = now
				fmt.Printf("Computed and saved the expansion of %d unionable columns in %.2f seconds\n", total.Values, now-start)
			}
		}
		fmt.Printf("Computed and saved the expansion of %d unionable columns in %.2f seconds\n", total.Values, GetNow()-start)
	*/
}
