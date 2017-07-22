package opendata

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path"
	"sync"

	minhashlsh "github.com/RJMillerLab/table-union/minhashlsh"
)

var (
	seed    = 1
	numHash = 256
)

type DomainSketch struct {
	Filename string              // the logical filename of the CSV file
	Index    int                 // the position of the domain in the csv file
	Sketch   *minhashlsh.Minhash // the minhash sketch
}

func DoMinhashDomainsFromFiles(fanout int, files <-chan string, ext string) <-chan *DomainSketch {
	out := make(chan *DomainSketch)
	wg := &sync.WaitGroup{}

	for i := 0; i < fanout; i++ {
		wg.Add(1)
		go func(id int) {
			for file := range files {
				for _, index := range getTextDomains(file) {
					minhashDomainWords(file, index, out, ext)
				}
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func minhashDomainWords(file string, index int, out chan *DomainSketch, ext string) {
	filepath := path.Join(OutputDir, "domains", file, fmt.Sprintf("%d.%s", index, ext))
	f, err := os.Open(filepath)
	if err != nil {
		return
		//panic(err)
	}
	defer f.Close()
	mh := minhashlsh.NewMinhash(seed, numHash)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		words := wordsFromLine(scanner.Text())
		for _, word := range words {
			mh.Push([]byte(word))
		}
	}
	out <- &DomainSketch{
		Filename: file,
		Index:    index,
		Sketch:   mh,
	}
}

func DoOntologyMinhashFromDB(fanout int, files <-chan string) <-chan *DomainSketch {
	out := make(chan *DomainSketch)
	wg := &sync.WaitGroup{}
	//defer db.Close()
	dbDomains := readDomainsWithOntologyFromDB()
	for i := 0; i < fanout; i++ {
		wg.Add(1)
		go func(id int) {
			for domain := range dbDomains {
				minhashDomainClasses(domain.tableName, domain.columnIndex, out)
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

type dbDomain struct {
	tableName   string
	columnIndex int
}

func readDomainsWithOntologyFromDB() <-chan dbDomain {
	out := make(chan dbDomain)
	db, err := sql.Open("sqlite3", AnnotationDB)
	if err != nil {
		panic(err)
	}
	//defer db.Close()
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT table_name, column_index FROM %s WHERE class_frequncy != 0;`, AllAnnotationTable))
	//rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT table_name, column_index FROM %s;`, AllAnnotationTableL2))
	if err != nil {
		panic(err)
	}
	go func() {
		for rows.Next() {
			var tableName string
			var columnIndex int
			err := rows.Scan(&tableName, &columnIndex)
			if err != nil {
				panic(err)
			}
			log.Printf("%s", tableName)
			out <- dbDomain{
				tableName:   tableName,
				columnIndex: columnIndex,
			}
		}
		rows.Close()
		db.Close()
		close(out)
	}()
	return out
}

func minhashDomainClasses(file string, index int, out chan *DomainSketch) {
	mh := minhashlsh.NewMinhash(seed, numHash)
	db, err := sql.Open("sqlite3", AnnotationDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	//rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT super_class FROM %s WHERE table_name="%s" AND column_index=%d;`, AllAnnotationTableL2, file, index))
	//if err != nil {
	//	panic(err)
	//}
	//for rows.Next() {
	//	var superClass string
	//	err := rows.Scan(&superClass)
	//	if err != nil {
	//		panic(err)
	//	}
	//	mh.Push([]byte(superClass))
	//}
	//rows.Close()
	//rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT class FROM %s WHERE table_name="%s" AND column_index=%d AND class_frequncy != 0;`, AllAnnotationTable, file, index))
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT class FROM %s WHERE table_name="%s" AND column_index=%d;`, AllAnnotationTable, file, index))
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var class string
		err := rows.Scan(&class)
		if err != nil {
			panic(err)
		}
		mh.Push([]byte(class))
	}
	rows.Close()
	out <- &DomainSketch{
		Filename: file,
		Index:    index,
		Sketch:   mh,
	}
}

// Saves the domain skecthes from an input channel to disk
// Returns a channel of progress counter
func DoSaveDomainSketches(fanout int, sketches <-chan *DomainSketch, ext string) <-chan ProgressCounter {
	progress := make(chan ProgressCounter)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int, sketches <-chan *DomainSketch) {
			for domain := range sketches {
				log.Printf("%s", domain.Filename)
				minhashFilename := domain.PhysicalFilename(ext)
				err := writeMinhashSignature(domain.Sketch, minhashFilename)
				if err == nil {
					log.Printf("%s", minhashFilename)
					progress <- ProgressCounter{1}
				}
			}
			wg.Done()
		}(i, sketches)
	}
	go func() {
		wg.Wait()
		close(progress)
	}()
	return progress
}

func (domain *DomainSketch) PhysicalFilename(ext string) string {
	fullpath := path.Join(OutputDir, "domains", domain.Filename)

	if ext != "" {
		fullpath = path.Join(fullpath, fmt.Sprintf("%d.%s", domain.Index, ext))
	}

	return fullpath
}

func writeMinhashSignature(mh *minhashlsh.Minhash, filename string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for i := range mh.Signature() {
		if err := binary.Write(f, binary.BigEndian, mh.Signature()[i]); err != nil {
			return err
		}
	}
	return nil
}

func ReadMinhashSignature(filename string, numHash int) ([]uint64, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
		//panic(err)
	}
	defer f.Close()
	signature := make([]uint64, numHash)
	for i := range signature {
		if err := binary.Read(f, binary.BigEndian, &(signature[i])); err != nil {
			return nil, err
		}
	}
	return signature, nil
}

func GetDomainMinhash(tokenFun func(string) []string, transFun func(string) string, column []string, numHash int) []uint64 {
	values := TokenizedValues(column, tokenFun, transFun)
	mh := minhashlsh.NewMinhash(seed, numHash)

	for tokens := range values {
		for _, word := range tokens {
			mh.Push([]byte(word))
		}
	}
	return mh.Signature()
}

// Produce a channel of values (tokenized)
func TokenizedValues(values []string, tokenFun func(string) []string, transFun func(string) string) chan []string {
	out := make(chan []string)
	go func() {
		for _, v := range values {
			v = transFun(v)
			// Tokenize
			tokens := tokenFun(v)
			if len(tokens) > 5 {
				// Skip text values
				continue
			}
			for i, t := range tokens {
				tokens[i] = transFun(t)
			}
			out <- tokens
		}
		close(out)
	}()
	return out
}

func StreamMinhashVectors(fanout int, ext string, filenames <-chan string) <-chan string {
	out := make(chan string)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func(id int, out chan<- string) {
			for filename := range filenames {
				for _, index := range getTextDomains(filename) {
					d := &Domain{
						Filename: filename,
						Index:    index,
					}
					minhashFilename := d.PhysicalFilename(ext)
					out <- minhashFilename
				}
			}
			wg.Done()
		}(i, out)

	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
