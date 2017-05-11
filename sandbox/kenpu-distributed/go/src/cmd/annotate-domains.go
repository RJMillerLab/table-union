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
	domains := StreamDomainValuesFromFiles(20, filenames)

	count := 0
	annotated := 0
	start = GetNow()
	for domain := range domains {
		for _, word := range domain.Values {
			cats := yago.Annotate(word)
			if cats != nil {
				annotated += 1
			}
		}
		count += 1
		if count%100 == 0 {
			fmt.Printf("Processed %d domains, annotated %d in %.2f seconds\n", count, annotated, GetNow()-start)
		}
	}
}
