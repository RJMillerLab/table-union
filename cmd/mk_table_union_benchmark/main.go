package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"unicode"

	opendata "github.com/RJMillerLab/go-opendata"
	"github.com/RJMillerLab/table-union/embedding"
	"github.com/ekzhu/counter"
)

var (
	databases = [][]string{
		[]string{"/home/ekzhu/OPENDATA/2017-06-05/open.canada.ca_data_en.jsonl.db", "canada"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/catalog.data.gov.jsonl.db", "us"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/data.gov.uk.jsonl.db", "uk"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/data.opencolorado.org.jsonl.db", "colorado"},
	}
	numBenchmarkTablePerRaw = 10
	fastTextMinNumCol       = 3
	fasttextMinPct          = 0.8
	maxNumRowBeforeGiveUp   = 10000
	maxSrcTableNumRow       = 1000000
	statTablename           = "dataset_profile"
)

type tableStat struct {
	*opendata.TableStat
	colStats []columnStat
}

type columnStat struct {
	nmapped int
}

func (stat tableStat) countNumFastTextCols() int {
	var n int
	for i := range stat.colStats {
		pct := float64(stat.colStats[i].nmapped) / float64(stat.NumRow)
		if pct >= fasttextMinPct {
			n++
		}
	}
	return n
}

func main() {
	var output string
	var numRawTableToSelect int
	var fastTextDatabaseFilename string
	flag.StringVar(&output, "output", "",
		"The output is a SQLite database storing the benchmark tables.")
	flag.IntVar(&numRawTableToSelect, "numSrc", 10,
		"The number of source tables to select for creating benchmark")
	flag.StringVar(&fastTextDatabaseFilename, "fasttext",
		"/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"The FastText database")
	flag.Parse()
	od, err := opendata.NewExplorer(output)
	if err != nil {
		panic(err)
	}
	// Cleaning cached tables
	log.Print("Cleaning previously generated tables...")
	cached, err := od.CachedTables()
	for _, name := range cached {
		if name == statTablename {
			continue
		}
		if err := od.DropCachedTable(name); err != nil {
			panic(err)
		}
	}

	// Attaching databases
	for _, database := range databases {
		log.Printf("Attach database %s as %s", database[0], database[1])
		if err := od.Attach(database[0], database[1]); err != nil {
			panic(err)
		}
	}

	// Compute table stats if it hasn't been done before
	if !od.CacheExists(statTablename) {
		log.Print("Computing table stats...")
		if err := od.Analyze("", statTablename); err != nil {
			panic(err)
		}
	}

	// Get the table stats
	log.Print("Loading table stats...")
	stats, err := od.GetTableStats(statTablename)
	if err != nil {
		panic(err)
	}
	log.Printf("Found %d tables", len(stats))

	// Sort the tables first by the number of rows and then by the number of columns
	// in descending order.
	log.Print("Sorting the table stats by nrow...")
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].NumRow == stats[j].NumRow {
			return stats[i].NumCol > stats[j].NumCol
		}
		return stats[i].NumRow > stats[j].NumRow
	})

	// Init FastText word set
	log.Printf("Loading FastText database from %s...", fastTextDatabaseFilename)
	ft, err := embedding.InitInMemoryFastText(fastTextDatabaseFilename,
		func(s string) []string {
			return strings.Split(s, " ")
		},
		func(s string) string {
			return strings.ToLower(strings.TrimFunc(strings.TrimSpace(s), unicode.IsPunct))
		})
	if err != nil {
		panic(err)
	}

	// Select tables to be used as the source tables
	log.Print("Selecting tables that have values map to FastText...")
	selected := make([]tableStat, 0)
	selectedPackages := counter.NewCounter()
	for s := range stats {
		if len(selected) == numRawTableToSelect {
			break
		}
		// Use the local wrapper
		stat := tableStat{&stats[s], nil}
		if selectedPackages.Has(stat.MetadataId) {
			log.Printf("Skipping table %s.%s as a sibling table was selected",
				stat.Database, stat.Name)
			continue
		}
		log.Printf("Scanning table %s.%s (%d rows, %d columns)...",
			stat.Database, stat.Name, stat.NumRow, stat.NumCol)
		err = od.ReadTable(stat.Database, stat.Name, func(rows *sql.Rows) error {
			// Figuring out how many columns and what are their types
			headers, err := rows.Columns()
			if err != nil {
				return err
			}
			stat.colStats = make([]columnStat, len(headers))
			colTypes, err := rows.ColumnTypes()
			if err != nil {
				return err
			}
			// Find out the text columns, so we are just looking at those
			textCols := make([]int, 0)
			for i, colType := range colTypes {
				if colType.DatabaseTypeName() == "TEXT" {
					textCols = append(textCols, i)
				}
			}
			// Scan the table on the text columns
			values := make([]sql.NullString, len(headers))
			ptrs := make([]interface{}, len(headers))
			for i := range ptrs {
				ptrs[i] = &values[i]
			}
			var currRowIndex int
			for rows.Next() {
				if err := rows.Scan(ptrs...); err != nil {
					panic(err)
				}
				for _, i := range textCols {
					if !values[i].Valid {
						continue
					}
					if _, err := ft.GetValueEmb(values[i].String); err == nil {
						stat.colStats[i].nmapped++
					}
				}
				currRowIndex++
				if currRowIndex == maxNumRowBeforeGiveUp && stat.countNumFastTextCols() == 0 {
					log.Printf("Found no FastText matches after scanning %d rows",
						maxNumRowBeforeGiveUp)
					break
				}
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		// Count number of columns meeting the criteria
		n := stat.countNumFastTextCols()
		if n >= fastTextMinNumCol {
			log.Printf("Selected %s.%s has %d columns with more than %.0f%% matches",
				stat.Database, stat.Name, n, fasttextMinPct*100.0)
			selected = append(selected, stat)
			selectedPackages.Update(stat.MetadataId)
		}
	}

	// Generating tablets
	log.Print("Generating benchmark tables from selected source tables...")
	for _, stat := range selected {
		limit := stat.NumRow / numBenchmarkTablePerRaw
		if limit == 0 {
			panic("Getting limit = 0")
		}
		for i := 0; i < numBenchmarkTablePerRaw; i++ {
			offset := limit * i
			tablename := fmt.Sprintf("%s____%s____%d", stat.Database, stat.Name, i)
			if err := od.LoadTableLimit(stat.Database, stat.Name, tablename, offset, limit); err != nil {
				panic(err)
			}
		}
	}

	// Done
	log.Print("Done")
}
