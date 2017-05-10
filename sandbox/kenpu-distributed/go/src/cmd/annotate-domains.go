package main

import (
	"fmt"
	. "opendata"
)

func main() {
	CheckEnv()

	start := GetNow()
	fmt.Println("Loading yago entities")
	yago := OpenYagoEntities()
	fmt.Printf("Yago loaded: %d entities in %.2f seconds\n", len(yago), GetNow()-start)

	filenames := StreamFilenames()
	domains := StreamDomainValuesFromFiles(1, filenames)

	count := 0
	for domain := range domains {
		if count > 1 {
			break
		}
		fmt.Printf("--- %s(%d) ---\n", domain.Filename, domain.Index)
		for i, word := range domain.Values {
			cats := yago.Annotate(word)
			fmt.Printf("  [%d] \"%s\" cats %d\n", i, word, len(cats))
		}
	}
}
