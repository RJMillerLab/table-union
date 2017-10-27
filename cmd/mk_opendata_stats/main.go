package main

import (
	"flag"
	"log"

	opendata "github.com/RJMillerLab/go-opendata"
	_ "github.com/mattn/go-sqlite3"
)

var (
	databases = [][]string{
		[]string{"/home/ekzhu/OPENDATA/2017-06-05/open.canada.ca_data_en.jsonl.db", "canada"},
		[]string{"/home/ekzhu/OPENDATA/2017-03-05/catalog.data.gov.jsonl.db", "us"},
		[]string{"/home/ekzhu/OPENDATA/2017-06-05/data.gov.uk.jsonl.db", "uk"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/data.opencolorado.org.jsonl.db", "colorado"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/datahub.io.jsonl.db", "datahub"},
	}
	statTablename = "dataset_profile"
)

func main() {
	var output string
	flag.StringVar(&output, "output", "",
		"The output is a SQLite database storing the benchmark tables.")
	flag.Parse()
	od, err := opendata.NewExplorer(output)
	if err != nil {
		panic(err)
	}

	// Attaching databases
	for _, database := range databases {
		log.Printf("Attach database %s as %s", database[0], database[1])
		if err := od.Attach(database[0], database[1]); err != nil {
			panic(err)
		}
	}

	// Compute table stats
	log.Print("Computing table stats...")
	if err := od.Analyze("", statTablename); err != nil {
		panic(err)
	}
}
