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
var opendata_dir = os.Getenv("OPENDATA_DIR")
var opendata_list = os.Getenv("OPENDATA_LIST")

// Environment variable required to
// write output
var output_dir = os.Getenv("OUTPUT_DIR")

// Environment variable for the Yago database
var Yago_db = os.Getenv("YAGO_DB")

func CheckEnv() {
	if opendata_dir == "" || output_dir == "" || Yago_db == "" {
		panic("Environment missing")
	}
}

func Filepath(filename string) string {
	return path.Join(opendata_dir, filename)
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
	f, err := os.Open(path.Join(output_dir, "word-entity.txt"))
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