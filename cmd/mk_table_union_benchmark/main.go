package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"
	"unicode"

	opendata "github.com/RJMillerLab/go-opendata"
	"github.com/RJMillerLab/table-union/embedding"
	"github.com/RJMillerLab/table-union/yago"
	"github.com/daviddengcn/go-algs/ed"
	"github.com/ekzhu/counter"
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
	numRawTableToSelect                   = 100
	numBenchmarkTablePerRaw               = 25
	fastTextMinNumCol                     = 4
	fasttextMinPct                        = 0.8
	yagoMinNumCol                         = fastTextMinNumCol
	yagoMinPct                            = fasttextMinPct
	maxSrcTableNumRow                     = 25000
	statTablename                         = "dataset_profile"
	maxNumDistinctBeforeGiveUp            = 100
	maxNumCharPerValue                    = 256
	minEditDistanceBetweenSelectedColumns = 5
	notAlphaNumeric                       = regexp.MustCompile("[^a-zA-Z0-9]+")
)

type tableStat struct {
	*opendata.TableStat
	colStats []columnStat
	columns  []string
}

type columnStat struct {
	distinctCount  int
	fastTextMapped int
	yagoMapped     int
}

func (colStat columnStat) isFastTextCol() bool {
	pct := float64(colStat.fastTextMapped) / float64(colStat.distinctCount)
	return pct >= fasttextMinPct
}

func (colStat columnStat) isYagoCol() bool {
	pct := float64(colStat.yagoMapped) / float64(colStat.distinctCount)
	return pct >= yagoMinPct
}

func (stat tableStat) metFastTextCriteria() bool {
	var n int
	for _, s := range stat.colStats {
		if s.isFastTextCol() {
			n++
		}
	}
	return n >= fastTextMinNumCol
}

func (stat tableStat) metYagoCriteria() bool {
	var n int
	for _, s := range stat.colStats {
		if s.isYagoCol() {
			n++
		}
	}
	return n >= yagoMinNumCol
}

func main() {
	var output string
	var fastTextDatabaseFilename string
	var yagoDatabaseFilename string
	var ignoreCoverage bool
	flag.StringVar(&output, "output", "",
		"The output is a SQLite database storing the benchmark tables.")
	flag.StringVar(&fastTextDatabaseFilename, "fasttext",
		"/home/ekzhu/FB_WORD_VEC/fasttext.db",
		"The FastText database")
	flag.StringVar(&yagoDatabaseFilename, "yago",
		"/home/ekzhu/YAGO/yago.sqlite",
		"The YAGO SQLite3 database")
	flag.BoolVar(&ignoreCoverage, "ignore-coverage", false,
		"Do not use YAGO and FastText coverage as selection criteria")
	flag.Parse()
	od, err := opendata.NewExplorer(output)
	if err != nil {
		panic(err)
	}
	// Cleaning cached tables
	log.Print("Cleaning previously generated tables...")
	cached, err := od.CachedTables()
	var countCached int
	for _, name := range cached {
		if name == statTablename {
			continue
		}
		countCached++
		fmt.Printf("\rDropping %d out of %d", countCached, len(cached)-1)
		if err := od.DropCachedTable(name); err != nil {
			panic(err)
		}
	}
	fmt.Println()

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

	var ft *embedding.FastText
	var yg *yago.Yago
	if !ignoreCoverage {
		// Init FastText word set
		log.Printf("Loading FastText database from %s...", fastTextDatabaseFilename)
		ft, err = embedding.InitInMemoryFastText(fastTextDatabaseFilename,
			func(s string) []string {
				return strings.Split(s, " ")
			},
			func(s string) string {
				return strings.ToLower(strings.TrimFunc(strings.TrimSpace(s), unicode.IsPunct))
			})
		if err != nil {
			panic(err)
		}
		defer ft.Close()

		// Init YAGO database
		log.Printf("Loading YAGO database from %s ...", yagoDatabaseFilename)
		yg = yago.InitYago(yagoDatabaseFilename)
		defer yg.Close()
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
			// log.Printf("Skipping table %s.%s as max number of rows exceeded",
			// 	stat.Database, stat.Name)
			continue
		}
		if stat.NumCol < fastTextMinNumCol || stat.NumCol < yagoMinNumCol {
			log.Printf("Skipping table %s.%s as too few columns",
				stat.Database, stat.Name)
			continue
		}
		if selectedPackages.Has(stat.MetadataID) {
			log.Printf("Skipping table %s.%s as a sibling table was selected",
				stat.Database, stat.Name)
			continue
		}
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
		if ignoreCoverage {
			// If we don't use YAGO and FastText coverage as selection criteria
			// We can early-decide based on what we have so far
			if len(textCols) >= fastTextMinNumCol || len(textCols) >= yagoMinNumCol {
				log.Printf("Selected %s.%s", stat.Database, stat.Name)
				selected = append(selected, stat)
				selectedPackages.Update(stat.MetadataID)
			} else {
				log.Printf("Skipped %s.%s as requirement not satisfied",
					stat.Database, stat.Name)
			}
			continue
		}
		// Collecting column stats for each text column
		log.Printf("Scanning table %s.%s (%d rows, %d columns)...",
			stat.Database, stat.Name, stat.NumRow, stat.NumCol)
		var readErr error
		for _, i := range textCols {
			column := columns[i]
			err = od.ReadColumnDistinct(stat.Database, stat.Name, column, func(rows *sql.Rows) error {
				var value sql.NullString
				for rows.Next() {
					if err := rows.Scan(&value); err != nil {
						return err
					}
					if !value.Valid {
						continue
					}
					stat.colStats[i].distinctCount++
					if stat.colStats[i].distinctCount == maxNumDistinctBeforeGiveUp && (!stat.colStats[i].isFastTextCol() || !stat.colStats[i].isYagoCol()) {
						break
					}
					if len(value.String) > maxNumCharPerValue {
						continue
					}
					// Check if all tokens can find a fast text match
					if _, err := ft.GetValueEmbStrict(value.String); err == nil {
						stat.colStats[i].fastTextMapped++
					}
					// Check if all tokens can be used to find a entity match
					if result := yg.MatchEntity(value.String, 1); len(result) > 0 {
						stat.colStats[i].yagoMapped++
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
		if stat.metYagoCriteria() && stat.metFastTextCriteria() {
			log.Printf("Selected %s.%s", stat.Database, stat.Name)
			selected = append(selected, stat)
			selectedPackages.Update(stat.MetadataID)
		} else {
			log.Printf("Skipped %s.%s as requirement not satisfied",
				stat.Database, stat.Name)
		}
	}

	// Generating tablets
	log.Print("Generating benchmark tables from selected source tables...")
	for rank, stat := range selected {
		limit := stat.NumRow / numBenchmarkTablePerRaw
		if limit == 0 {
			panic("Getting limit = 0")
		}
		// First load the original table into the cache in random order
		// of rows
		originalTablename := fmt.Sprintf("%d____%s____%s", rank, stat.Database, stat.Name)
		if err := od.LoadTable(stat.Database, stat.Name, originalTablename); err != nil {
			panic(err)
		}
		for i := 0; i < numBenchmarkTablePerRaw; i++ {
			offset := limit * i
			tablename := fmt.Sprintf("%d____%s____%s____%d", rank, stat.Database, stat.Name, i)
			if err := od.LoadTableLimit("", originalTablename, tablename, offset, limit); err != nil {
				panic(err)
			}
		}
	}

	// Done
	log.Print("Done")
}
