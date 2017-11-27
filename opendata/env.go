package opendata

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const PARALLEL = 64
const MIN_DOMSIZE = 5

// Environment variables required to
// locate the necessary input files
var OpendataDir = os.Getenv("OPENDATA_DIR")
var OpendataList = os.Getenv("OPENDATA_LIST")

// Environment variable required to
// write output
var OutputDir = os.Getenv("OUTPUT_DIR")

// Environment variable for the Yago database
var Yago_db = os.Getenv("YAGO_DB")

// Environment variable for domain pairs unionability scores
var SarmaFilename = os.Getenv("SARMA_SCORES")
var JaccardFilename = os.Getenv("JACCARD_SCORES")
var ContainmentFilename = os.Getenv("CONTAINMENT_SCORES")
var CosineFilename = os.Getenv("COSINE_SCORES")
var ColumnUnionabilityTable = os.Getenv("COLUMN_UNIONABILITY_TABLE")
var DatasetUnionabilityTable = os.Getenv("DATASET_UNIONABILITY_TABLE")
var ColumnUnionabilityDB = os.Getenv("COLUMN_UNIONABILITY_DB")
var DatasetUnionabilityDB = os.Getenv("DATASET_UNIONABILITY_DB")
var QueryList = os.Getenv("QUERY_LIST")
var QueryList2 = os.Getenv("QUERY_LIST2")
var Measure = os.Getenv("MEASURE")
var Threshold = os.Getenv("THRESHOLD")
var AnnotationDB = os.Getenv("ANNOTATION_DB")
var SubjectAnnotationTable = os.Getenv("SUBJECT_ANNOTATION_TABLE")
var AllAnnotationTable = os.Getenv("ALL_ANNOTATION_TABLE")
var SarmaTable = os.Getenv("SARMA_TABLE")
var SarmaDB = os.Getenv("SARMA_DB")
var AllAnnotationTableL2 = os.Getenv("ALL_ANNOTATION_TABLE_L2")
var OctopusTable = os.Getenv("OCTOPUS_TABLE")
var OctopusDB = os.Getenv("OCTOPUS_DB")
var AttStatsTable = os.Getenv("ATT_STATS_TABLE")
var AttStatsDB = os.Getenv("ATT_STATS_DB")
var TableStatsTable = os.Getenv("TABLE_STATS_TABLE")
var TableStatsDB = os.Getenv("TABLE_STATS_DB")
var CTableStatsDB = os.Getenv("C_TABLE_STATS_DB")
var CTableStatsTable = os.Getenv("C_TABLE_STATS_TABLE")
var AttCDFTable = os.Getenv("ATT_CDF_TABLE")
var TableCDFTable = os.Getenv("TABLE_CDF_TABLE")
var AllAttStatsTable = os.Getenv("ALL_ATT_STATS_TABLE")
var SetCDFTable = os.Getenv("SET_CDF_TABLE")
var SemCDFTable = os.Getenv("SEM_CDF_TABLE")
var SemSetCDFTable = os.Getenv("SEMSET_CDF_TABLE")
var NlCDFTable = os.Getenv("NL_CDF_TABLE")
var AllAttPercentileTable = os.Getenv("ALL_ATT_PERCENTILE_TABLE")

func CheckEnv() {
}

func Filepath(filename string) string {
	return path.Join(OpendataDir, filename)
}

func GetNow() float64 {
	return float64(time.Now().UnixNano()) / 1E9
}

func GenericStrings(s []string) []interface{} {
	var a = make([]interface{}, len(s))
	for i := 0; i < len(s); i++ {
		a[i] = s[i]
	}
	return a
}

func LoadEntityWords() map[string]map[string]bool {
	lookup := make(map[string]map[string]bool)
	f, err := os.Open(path.Join(OutputDir, "word-entity.txt"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	i := 0
	start := GetNow()

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "|", 2)
		word, entity := parts[0], parts[1]
		if _, ok := lookup[word]; !ok {
			lookup[word] = make(map[string]bool)
		}
		lookup[word][entity] = true
		i += 1
		if i%100000 == 0 {
			fmt.Printf("LoadEntityWords: %d in %.2f seconds\n", i, GetNow()-start)
		}
	}
	return lookup
}
func LoadEntityWordCount() map[string]int {
	counts := make(map[string]int)
	db, err := sql.Open("sqlite3", Yago_db)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(`select entity, words_count from words_count`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var ent string
		var count int
		if err = rows.Scan(&ent, &count); err != nil {
			panic(err)
		}
		counts[ent] = count
		i += 1
		if i%100000 == 0 {
			fmt.Printf("LoadEntityWordCount: %d\n", i)
		}
	}
	return counts
}
