package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"unicode"

	_ "github.com/mattn/go-sqlite3"

	"github.com/RJMillerLab/table-union/embedding"
	"github.com/ekzhu/counter"
)

var (
	DefaultTransFun = func(s string) string {
		return strings.ToLower(strings.TrimFunc(strings.TrimSpace(s), unicode.IsPunct))
	}
	DefaultTokenFun = func(s string) []string { return strings.Split(s, " ") }
)

type ProfResult struct {
	Filename        string
	FastTextMatches []FastTextMatch
}

type FastTextMatch struct {
	NumValues        int
	NumMatches       int
	NumUniqueValues  int
	NumUniqueMatches int
}

func profTable(rows <-chan []string, fastTextWords *counter.ConcurrentCounter) ([]FastTextMatch, error) {
	var nrow, ncol int
	var valueCounters []*counter.Counter
	var matchCounters []*counter.Counter
	for row := range rows {
		if ncol == 0 && valueCounters == nil && matchCounters == nil {
			// Initialize the counters
			ncol = len(row)
			valueCounters = make([]*counter.Counter, ncol)
			matchCounters = make([]*counter.Counter, ncol)
			for i := 0; i < ncol; i++ {
				valueCounters[i] = counter.NewCounter()
				matchCounters[i] = counter.NewCounter()
			}
			// Skip the header row
			continue
		}
		for i, value := range row {
			// Count value
			valueCounters[i].Update(value)
			// Count match
			if matchCounters[i].Has(value) {
				matchCounters[i].Update(value)
				continue
			}
			// If one token has a fasttext match, the value has a match
			tokens := embedding.Tokenize(value, DefaultTokenFun, DefaultTransFun)
			found := false
			for _, token := range tokens {
				if fastTextWords.Has(token) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
			matchCounters[i].Update(value)
		}
		nrow++
	}
	if valueCounters == nil && matchCounters == nil {
		return nil, errors.New("Empty table")
	}
	matches := make([]FastTextMatch, ncol)
	for i := range matches {
		matches[i] = FastTextMatch{
			NumValues:        valueCounters[i].Total(),
			NumUniqueValues:  valueCounters[i].Unique(),
			NumMatches:       matchCounters[i].Total(),
			NumUniqueMatches: matchCounters[i].Unique(),
		}
	}
	return matches, nil
}

func readCsv(csvFile io.Reader) (<-chan []string, <-chan error) {
	out := make(chan []string)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		reader := csv.NewReader(csvFile)
		for {
			rec, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				errc <- err
				return
			}
			out <- rec
		}
	}()
	return out, errc
}

// Where we profile the table
func ProcessTable(datafilename string, fastTextWords *counter.ConcurrentCounter, out chan<- *ProfResult) error {
	datafile, err := os.Open(datafilename)
	if err != nil {
		return err
	}
	defer datafile.Close()
	recChan, errc := readCsv(datafile)
	matches, err := profTable(recChan, fastTextWords)
	if err := <-errc; err != nil {
		return err
	}
	if err != nil {
		return err
	}
	out <- &ProfResult{
		Filename:        datafilename,
		FastTextMatches: matches,
	}
	return nil
}

func SaveProfResult(dbFilename string, results <-chan *ProfResult) error {
	db, err := sql.Open("sqlite3", dbFilename)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`
	DROP TABLE IF EXISTS matchfasttext;
	CREATE TABLE matchfasttext (
		datafile TEXT,
		column_index INTEGER,
		num_values INTEGER,
		num_unique_values INTEGER,
		num_matches INTEGER,
		num_unique_matches INTEGER
	);`)
	if err != nil {
		return err
	}
	stmt, err := db.Prepare(`
	INSERT INTO matchfasttext(datafile, column_index, num_values, num_unique_values, num_matches, num_unique_matches) VALUES(?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for result := range results {
		for i, match := range result.FastTextMatches {
			// Insert into database
			_, err = stmt.Exec(
				result.Filename,
				i,
				match.NumValues,
				match.NumUniqueValues,
				match.NumMatches,
				match.NumUniqueMatches,
			)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	var fastTextDbFilename string
	var fileListFilename string
	var profFilename string
	var nworker int
	flag.StringVar(&fastTextDbFilename, "fasttext", "", "Location of the fasttext database file")
	flag.StringVar(&fileListFilename, "filelist", "", "Input list of dataset files")
	flag.StringVar(&profFilename, "output", "", "Output of profing result as a SQLite3 database file")
	flag.IntVar(&nworker, "thread", 4, "Number of parallel worker threads")
	flag.Parse()

	fileListFile, err := os.Open(fileListFilename)
	if err != nil {
		panic(err)
	}
	defer fileListFile.Close()
	log.Printf("Using file list from %s", fileListFilename)

	// Loading fasttext database
	ft, err := embedding.InitFastText(fastTextDbFilename, DefaultTokenFun, DefaultTransFun)
	if err != nil {
		panic(err)
	}
	fastTextWords := counter.NewConcurrentCounter()
	words, err := ft.GetAllWords()
	if err != nil {
		panic(err)
	}
	for _, word := range words {
		fastTextWords.Update(word)
	}
	ft.Close()
	log.Printf("Fasttext words loaded from %s", fastTextDbFilename)

	// Start parallel profiling threads
	inputChan, errc := readCsv(fileListFile)
	go func() {
		if err := <-errc; err != nil {
			panic(err)
		}
	}()
	outChan := make(chan *ProfResult)
	var wg sync.WaitGroup
	wg.Add(nworker)
	for i := 0; i < nworker; i++ {
		go func() {
			defer wg.Done()
			for datafileRec := range inputChan {
				datafilename := datafileRec[0]
				// Skipping french dataset files
				if strings.HasSuffix(datafilename, "-fra.csv") {
					continue
				}
				log.Printf("Processing %s", datafilename)
				err := ProcessTable(datafilename, fastTextWords, outChan)
				if err != nil {
					log.Printf("Error in table %s: %s", datafilename, err.Error())
					continue
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(outChan)
	}()

	// Save profile result to SQLite3 database
	if err := SaveProfResult(profFilename, outChan); err != nil {
		panic(err)
	}
}
