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
	"github.com/daviddengcn/go-algs/ed"
	"github.com/ekzhu/counter"
)

var (
	databases = [][]string{
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/open.canada.ca_data_en.jsonl.db", "canada"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/catalog.data.gov.jsonl.db", "us"},
		[]string{"/home/ekzhu/OPENDATA/2017-06-05/data.gov.uk.jsonl.db", "uk"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/data.opencolorado.org.jsonl.db", "colorado"},
		// []string{"/home/ekzhu/OPENDATA/2017-06-05/datahub.io.jsonl.db", "datahub"},
	}
	numRawTableToSelect                   = 200
	numBenchmarkTablePerRaw               = 25
	fastTextMinNumCol                     = 3
	fasttextMinPct                        = 0.8
	maxSrcTableNumRow                     = 1000000
	statTablename                         = "dataset_profile"
	maxNumDistinctBeforeGiveUp            = 100
	maxNumCharPerValue                    = 256
	minEditDistanceBetweenSelectedColumns = 5
)

type tableStat struct {
	*opendata.TableStat
	colStats []columnStat
	columns  []string
}

type columnStat struct {
	distinctCount int
	nmapped       int
}

func (stat tableStat) isFastTextCol(i int) bool {
	pct := float64(stat.colStats[i].nmapped) / float64(stat.colStats[i].distinctCount)
	return pct >= fasttextMinPct
}

func (stat tableStat) countNumFastTextCols() int {
	var n int
	for i := range stat.colStats {
		if stat.isFastTextCol(i) {
			n++
		}
	}
	return n
}

func main() {
	var output string
	var fastTextDatabaseFilename string
	flag.StringVar(&output, "output", "",
		"The output is a SQLite database storing the benchmark tables.")
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
		stat := tableStat{&stats[s], nil, nil}
		if stat.NumRow > maxSrcTableNumRow {
			log.Printf("Skipping table %s.%s as max number of rows exceeded",
				stat.Database, stat.Name)
			continue
		}
		if stat.NumCol < fastTextMinNumCol {
			log.Printf("Skipping table %s.%s as to few columns",
				stat.Database, stat.Name)
			continue
		}
		if selectedPackages.Has(stat.MetadataId) {
			log.Printf("Skipping table %s.%s as a sibling table was selected",
				stat.Database, stat.Name)
			continue
		}
		log.Printf("Scanning table %s.%s (%d rows, %d columns)...",
			stat.Database, stat.Name, stat.NumRow, stat.NumCol)
		// Get the column names and their types
		columns, colTypes, err := od.GetColumns(stat.Database, stat.Name)
		if err != nil {
			panic(err)
		}
		stat.columns = columns
		stat.colStats = make([]columnStat, len(columns))
		// Check if we have seen very similar column names
		var similarTable *tableStat
		for j := range selected {
			d := ed.EditDistanceF(len(columns), len(selected[j].columns),
				func(iA, iB int) int {
					return ed.Ternary(strings.ToLower(columns[iA]) == strings.ToLower(selected[j].columns[iB]), 0, 1)
				},
				ed.ConstCost(1), ed.ConstCost(1))
			if d < minEditDistanceBetweenSelectedColumns {
				similarTable = &selected[j]
				break
			}
		}
		if similarTable != nil {
			log.Printf("Skipping table %s.%s as having similar columns as %s.%s",
				stat.Database, stat.Name, similarTable.Database, similarTable.Name)
			continue
		}
		// Find out the text columns, so we are just looking at those
		textCols := make([]int, 0)
		for i, colType := range colTypes {
			if colType.DatabaseTypeName() == "TEXT" {
				textCols = append(textCols, i)
			}
		}
		// Collecting column stats for each text column
		var readErr error
		for _, i := range textCols {
			column := columns[i]
			err = od.ReadColumnDistinct(stat.Database, stat.Name, column, func(rows *sql.Rows) error {
				var value sql.NullString
				var count int
				for rows.Next() {
					if err := rows.Scan(&value); err != nil {
						return err
					}
					if !value.Valid {
						continue
					}
					count++
					if count == maxNumDistinctBeforeGiveUp && !stat.isFastTextCol(i) {
						break
					}
					if len(value.String) > maxNumCharPerValue {
						continue
					}
					if _, err := ft.GetValueEmb(value.String); err == nil {
						stat.colStats[i].nmapped++
					}
				}
				return nil
			})
			if err != nil {
				readErr = err
				break
			}
		}
		if readErr != nil {
			log.Printf("Skipped %s.%s as error: %s",
				stat.Database, stat.Name, readErr.Error())
			continue
		}
		// Count number of columns meeting the criteria
		n := stat.countNumFastTextCols()
		if n >= fastTextMinNumCol {
			log.Printf("Selected %s.%s has %d columns with more than %.0f%% matches",
				stat.Database, stat.Name, n, fasttextMinPct*100.0)
			selected = append(selected, stat)
			selectedPackages.Update(stat.MetadataId)
		} else {
			log.Printf("Skipped %s.%s as requirement not satisfied",
				stat.Database, stat.Name)
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
