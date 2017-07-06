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
)

var (
	databases = [][]string{
		[]string{"/home/ekzhu/OPENDATA/2017-06-05/open.canada.ca_data_en.jsonl.db", "canada"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/catalog.data.gov.jsonl.db", "us"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/data.gov.uk.jsonl.db", "uk"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/data.opencolorado.org.jsonl.db", "colorado"},
	}
	numBenchmarkTablePerRaw = 10
	fastTextMinNumCol       = 5
	fasttextMinPct          = 0.8
	maxNumRowBeforeGiveUp   = 100
)

type tableStat struct {
	info     *opendata.TableInfo
	nrow     int
	ncol     int
	colStats []columnStat
}

type columnStat struct {
	nmapped int
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
	// Attaching databases
	for _, database := range databases {
		log.Printf("Attach database %s as %s", database[0], database[1])
		if err := od.Attach(database[0], database[1]); err != nil {
			panic(err)
		}
	}

	// Get table infos
	log.Print("Getting Open Data table infos...")
	tables, err := od.GetTableInfos("")
	if err != nil {
		panic(err)
	}
	log.Printf("Found %d tables", len(tables))

	// Compute table stats
	log.Print("Computing table stats...")
	stats := make([]tableStat, len(tables))
	for i := range stats {
		nrow, err := od.CountRows(tables[i].Database, tables[i].Name)
		if err != nil {
			panic(err)
		}
		ncol, err := od.CountCols(tables[i].Database, tables[i].Name)
		if err != nil {
			panic(err)
		}
		stats[i] = tableStat{
			info: &tables[i],
			ncol: ncol,
			nrow: nrow,
		}
	}

	// Sort the tables first by the number of rows and then by the number of columns
	// in descending order.
	log.Print("Sorting the table stats by nrow...")
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].nrow == stats[j].nrow {
			return stats[i].ncol > stats[j].ncol
		}
		return stats[i].nrow > stats[j].nrow
	})

	// Init FastText
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
	for _, stat := range stats {
		if len(selected) == numRawTableToSelect {
			break
		}
		log.Printf("Scanning table %s.%s (%d rows, %d columns)...",
			stat.info.Database, stat.info.Name, stat.nrow, stat.ncol)
		err = od.ReadTable(stat.info.Database, stat.info.Name, func(rows *sql.Rows) error {
			headers, err := rows.Columns()
			if err != nil {
				return err
			}
			colTypes, err := rows.ColumnTypes()
			if err != nil {
				return err
			}
			textCols := make([]int, 0)
			for i, colType := range colTypes {
				if colType.DatabaseTypeName() == "TEXT" {
					textCols = append(textCols, i)
				}
			}
			colStats := make([]columnStat, len(headers))
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
						colStats[i].nmapped++
					}
				}
				currRowIndex++
				// If no FastText matches found at this point, give up scanning
				// further for this table.
				if currRowIndex == maxNumRowBeforeGiveUp {
					var sum int
					for i := range colStats {
						sum += colStats[i].nmapped
					}
					if sum == 0 {
						log.Printf("Found no FastText matches after scanning %d rows",
							maxNumRowBeforeGiveUp)
						break
					}
				}
			}
			stat.colStats = colStats
			return nil
		})
		if err != nil {
			panic(err)
		}
		// Count number of columns meeting the criteria
		var n int
		for i := range stat.colStats {
			pct := float64(stat.colStats[i].nmapped) / float64(stat.nrow)
			if pct >= fasttextMinPct {
				n++
			}
		}
		log.Printf("%s.%s has %d columns with more than %.2f% matches",
			stat.info.Database, stat.info.Name, n, fasttextMinPct)
		if n >= fastTextMinNumCol {
			selected = append(selected, stat)
		}
	}

	// Generating tablets
	log.Print("Generating benchmark tables from selected source tables...")
	for _, stat := range selected {
		limit := stat.nrow / numBenchmarkTablePerRaw
		if limit == 0 {
			panic("Getting limit = 0")
		}
		for i := 0; i < numBenchmarkTablePerRaw; i++ {
			offset := limit * i
			tablename := fmt.Sprintf("%s____%s____%d", stat.info.Database, stat.info.Name, i)
			if err := od.LoadTableLimit(stat.info.Database, stat.info.Name, tablename, offset, limit); err != nil {
				panic(err)
			}
		}
	}

	// Done
	log.Print("Done")
}
