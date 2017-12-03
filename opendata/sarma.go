package opendata

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/RJMillerLab/table-union/minhashlsh"
	"github.com/RJMillerLab/table-union/pqueue"
	"github.com/RJMillerLab/table-union/yago"
	"github.com/ekzhu/counter"
)

var (
	entityToClass map[string][]string
	n1            = 2.0
	m1            = 2.0
	n2            = 2.0
	m2            = 2.0
)

type domainAnnotation struct {
	filename    string
	index       int
	classes     map[string]int
	numEntities int
}

func InitSarma() {
	entityToClass = loadEntityClasses()
}

func InitAnnotator() {
	entityToClass = loadEntityClasses()
	//prepareDB()
}

func AnnotateDomainsFromEntityFiles(files <-chan string, fanout int, ext string) <-chan *domainAnnotation {
	out := make(chan *domainAnnotation, 1000)
	wg := &sync.WaitGroup{}

	for i := 0; i < fanout; i++ {
		wg.Add(1)
		go func(id int) {
			for file := range files {
				//subjectColumn := getSubjectColumn(file)
				textDomains := getTextDomains(file)
				for _, index := range textDomains {
					annotateDomainEntities(file, index, out, ext)
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

func getSubjectColumn(tablename string) int {
	db, err := sql.Open("sqlite3", AnnotationDB)
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT subject_column FROM %s where table_name="%s";`, AllAnnotationTable, tablename))
	if err != nil {
		panic(err)
	}
	//
	subjectColumn := -1
	for rows.Next() {
		err := rows.Scan(&subjectColumn)
		if err != nil {
			panic(err)
		}
	}
	db.Close()
	return subjectColumn
}

func getSubjectColumnPlus(tableName string) int {
	// assuming the first text column (non-numerical and non-date)
	// with annotations as subject column
	textDomains := getTextDomains(tableName)
	if len(textDomains) == 0 {
		return -1
	}
	for _, i := range textDomains {
		filepath := path.Join(OutputDir, "domains", tableName, fmt.Sprintf("%d.entities", i))
		file, err := os.Open(filepath)
		if err == nil {
			//if _, err := os.Stat(filepath); os.IsExist(err) {
			return i
		}
		file.Close()
	}
	return -1
}

func annotateDomainEntities(file string, index int, out chan *domainAnnotation, ext string) {
	// this is here just to restore annotation for unannotated domains
	if _, err := os.Stat(path.Join(OutputDir, "domains", file, fmt.Sprintf("%d.%s", index, "ont-minhash-l1"))); !os.IsNotExist(err) {
		return
	}

	filepath := path.Join(OutputDir, "domains", file, fmt.Sprintf("%d.%s", index, ext))
	f, err := os.Open(filepath)
	if err != nil {
		return
	}
	//defer f.Close()
	scanner := bufio.NewScanner(f)
	classes := make(map[string]int)
	numEntities := 0
	for scanner.Scan() {
		e := strings.ToLower(scanner.Text())
		if len(entityToClass[e]) == 0 {
			log.Printf("class not found for entity %s", e)
		} else {
			numEntities += 1
			for _, c := range entityToClass[e] {
				if _, ok := classes[c]; !ok {
					classes[c] = 1
				} else {
					classes[c] = classes[c] + 1
				}
			}
		}
	}
	if numEntities == 0 {
		log.Printf("numEntities = 0")
	}
	out <- &domainAnnotation{
		filename:    file,
		index:       index,
		classes:     classes,
		numEntities: numEntities,
	}
	f.Close()
	log.Printf("done annotating")
}

func DoSaveAnnotations(annotations <-chan *domainAnnotation) <-chan ProgressCounter {
	db, err := sql.Open("sqlite3", AnnotationDB)
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(table_name, column_index, column_name, class, class_frequncy, num_entities) values(?, ?, ?, ?, ?, ?);`, AllAnnotationTable))
	//stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(table_name, column_index, column_name, class, class_frequncy, num_entities) values(?, ?, ?, ?, ?, ?);`, SubjectAnnotationTable))
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	progress := make(chan ProgressCounter)
	go func() {
		for annotation := range annotations {
			log.Printf("saving annotations of %s", annotation.filename)
			subjectName := GetDomainHeader(annotation.filename).Values[annotation.index]
			if len(annotation.classes) == 0 {
				log.Printf("No annotation for attribute %s.%d", annotation.filename, annotation.index)
				_, err = stmt.Exec(annotation.filename, annotation.index, subjectName, "-1", 0, 0)
				if err != nil {
					panic(err)
				}
			}
			if len(annotation.classes) != 0 {
				// saving to a file
				cardFilename := path.Join(OutputDir, "domains", annotation.filename, fmt.Sprintf("%d.%s", annotation.index, "ont-card"))
				log.Printf("saving ontcard of %s", cardFilename)
				f, err := os.OpenFile(cardFilename, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					panic(err)
				}
				log.Printf("ont-card: %d", len(annotation.classes))
				fmt.Fprintln(f, len(annotation.classes))
				f.Close()
			}
			for class, freq := range annotation.classes {
				_, err = stmt.Exec(annotation.filename, annotation.index, subjectName, class, freq, annotation.numEntities)
				if err != nil {
					panic(err)
				}
			}
			progress <- ProgressCounter{1}
		}
		wg.Done()
	}()
	go func() {
		wg.Wait()
		db.Close()
		close(progress)
	}()
	return progress
}

func (annotation *domainAnnotation) saveToFile() {
	if len(annotation.classes) == 0 {
		return
	}
	dirPath := path.Join(OutputDir, "domains", annotation.filename)
	filepath := path.Join(dirPath, fmt.Sprintf("%d.annos-l1", annotation.index))

	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0644)
	//defer f.Close()

	if err == nil {
		for c, freq := range annotation.classes {
			fmt.Fprintln(f, c+" "+strconv.Itoa(freq))
		}
	} else {
		panic(fmt.Sprintf("Unable to save: %s", err.Error()))
	}
	f.Close()
	return
}

func (annotation *domainAnnotation) saveToDB() {
	// Prepare insert stmt
	db, err := sql.Open("sqlite3", AnnotationDB)
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(
		`insert into %s(table_name, column_index, column_name, class, class_frequncy, num_entities) values(?, ?, ?, ?, ?, ?);`, AllAnnotationTable))
	if err != nil {
		panic(err)
	}
	subjectName := GetDomainHeader(annotation.filename).Values[annotation.index]
	if len(annotation.classes) == 0 {
		_, err = stmt.Exec(annotation.filename, annotation.index, subjectName, "-1", 0, 0)
		if err != nil {
			panic(err)
		}
		return
	}
	for class, freq := range annotation.classes {
		_, err = stmt.Exec(annotation.filename, annotation.index, subjectName, class, freq, annotation.numEntities)
		if err != nil {
			panic(err)
		}
	}
	return
}

func loadEntityClasses() map[string][]string {
	lookup := make(map[string][]string)
	f, err := os.Open(path.Join(OutputDir, "entity-category.txt"))
	//f, err := os.Open(path.Join(OutputDir, "entity-class.txt"))
	if err != nil {
		panic(err)
	}
	//defer f.Close()
	scanner := bufio.NewScanner(f)
	i := 0
	start := GetNow()
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "|", 2)
		entity, class := strings.ToLower(parts[0]), parts[1]
		if _, ok := lookup[entity]; !ok {
			lookup[entity] = make([]string, 0)
		}
		lookup[entity] = append(lookup[entity], class)
		i += 1
		if i%100000 == 0 {
			fmt.Printf("LoadEntityClasses: %d in %.2f seconds\n", i, GetNow()-start)
		}
	}
	log.Printf("number of entities: %d", len(lookup))
	f.Close()
	return lookup
}

func prepareDB() {
	// output db
	db, err := sql.Open("sqlite3", AnnotationDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (table_name text,column_index int, column_name text, class text, class_frequncy int, num_entities int);`, AllAnnotationTable, AllAnnotationTable))
	//_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (table_name text, column_index int, column_name text, class text, class_frequncy int, num_entities int);`, SubjectAnnotationTable, SubjectAnnotationTable))
	if err != nil {
		panic(err)
	}
}

func bucketize(queryFilename string) <-chan string {
	candidateTables := make(chan string)
	subjectColumn := getSubjectColumn(queryFilename)
	if subjectColumn == -1 {
		log.Printf("No subject column was found for query %s", queryFilename)
		return candidateTables
	}
	go func() {
		// output db
		db, err := sql.Open("sqlite3", AnnotationDB)
		if err != nil {
			panic(err)
		}
		rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT a2.table_name as candidate_table FROM %s a2, (SELECT * FROM %s WHERE table_name="%s") a1 WHERE a2.subject_column=a2.column_index and (a1.column_name=a2.column_name OR a1.class=a2.class);`, AllAnnotationTable, AllAnnotationTable, queryFilename))
		if err != nil {
			panic(err)
		}
		//
		count := 0
		for rows.Next() {
			var candidateTable string
			err := rows.Scan(&candidateTable)
			if err != nil {
				panic(err)
			}
			candidateTables <- candidateTable
			count += 1
		}
		log.Printf("buck size: %d", count)
		rows.Close()
		db.Close()
		close(candidateTables)
	}()
	return candidateTables

}

type pair struct {
	queryColIndex     int
	candidateColIndex int
}

func readEntities(tableName string, columnIndex int) []string {
	entities := make([]string, 0)
	filepath := path.Join(OutputDir, "domains", tableName, fmt.Sprintf("%d.ont-minhash-l1", columnIndex))
	f, err := os.Open(filepath)
	if err != nil {
		return []string{}
	}
	//defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		e := scanner.Text()
		entities = append(entities, e)
	}
	f.Close()
	return entities
}

func shareEntities(queryFilename, candidateFilename string, queryTextColumns, candidateTextColumns []int) bool {
	queryEntities := readEntities(queryFilename, getSubjectColumn(queryFilename))
	candidateEntities := readEntities(candidateFilename, getSubjectColumn(candidateFilename))
	for _, e1 := range queryEntities {
		for _, e2 := range candidateEntities {
			if strings.ToLower(e1) == strings.ToLower(e2) {
				return true
			}
		}
	}
	return false
}

func computeSchemaConsistency(queryFilename, candidateFilename string, queryTextColumns, candidateTextColumns []int) float64 {
	// topK queue is used to find max weight matching
	schemaMapping := pqueue.NewTopKQueue(len(queryTextColumns) * len(candidateTextColumns))
	// for each pair of columns read labels, tokenize and compute fuzzy jaccard
	haveSharedEntities := shareEntities(queryFilename, candidateFilename, queryTextColumns, candidateTextColumns)
	for iqtc, qtc := range queryTextColumns {
		for ictc, ctc := range candidateTextColumns {
			matchingScore := computeLabelsFuzzyJaccard(queryFilename, qtc, candidateFilename, ctc)
			if matchingScore != 0.0 {
				if haveSharedEntities {
					valueConsistency := computeValueConsistency(queryFilename, candidateFilename, iqtc, ictc)
					log.Printf("valueConsistency: %f", valueConsistency)
					if valueConsistency >= 0.8 {
						p := pair{
							queryColIndex:     qtc,
							candidateColIndex: ctc,
						}
						schemaMapping.Push(p, matchingScore)
						continue
					}
				}
			}
			p := pair{
				queryColIndex:     qtc,
				candidateColIndex: ctc,
			}
			schemaMapping.Push(p, matchingScore)
		}
	}
	// mapping weight
	W := 0.0
	// compute max weight matching
	queryMatched := make(map[int]bool)
	candidateMatched := make(map[int]bool)
	pairs, scores := schemaMapping.Descending()
	for i, pi := range pairs {
		p := pi.(pair)
		if _, ok := queryMatched[p.queryColIndex]; !ok {
			if _, ok := queryMatched[p.candidateColIndex]; !ok {
				queryMatched[p.queryColIndex] = true
				candidateMatched[p.candidateColIndex] = true
				W += scores[i]
			}
		}
	}
	// no match found
	if len(queryMatched) == 0 {
		return 0.0
	}
	mappingScore := W / float64(len(queryTextColumns)+len(candidateTextColumns)-len(queryMatched))
	return mappingScore
}

func makeDomainAnnotationCounterFromDB(tableName string, columnIndex int) *counter.Counter {
	db, err := sql.Open("sqlite3", AnnotationDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// read and tokenize the labels of the query
	rows, err := db.Query(fmt.Sprintf(`SELECT column_name, class FROM %s WHERE table_name = "%s" AND        column_index = %d;`, AllAnnotationTable, tableName, columnIndex))
	if err != nil {
		panic(err)
	}
	// Get the name of the column
	var cName string
	domainCounter := counter.NewCounter()
	for rows.Next() {
		var anno string
		var columnName string
		err := rows.Scan(&columnName, &anno)
		if err != nil {
			panic(err)
		}
		if cName == "" {
			domainCounter.Update(columnName)
			cName = columnName
		}
		//words := tokenizeAnnotation(anno)
		//for word := range words {
		//	domainCounter.Update(word)
		//}
		domainCounter.Update(anno)
	}
	rows.Close()
	return domainCounter
}

func makeDomainEntityCounter(tableName string, columnIndex int) (*counter.Counter, error) {
	filepath := path.Join(OutputDir, "domains", tableName, fmt.Sprintf("%d.entities-l0", columnIndex))
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	//defer f.Close()
	scanner := bufio.NewScanner(f)
	domainEntityCounter := counter.NewCounter()
	for scanner.Scan() {
		e := scanner.Text()
		domainEntityCounter.Update(e)
	}
	f.Close()
	return domainEntityCounter, nil
}

func computeLabelsFuzzyJaccard(queryFilename string, queryColumnIndex int, candidateFilename string, candidateColumnIndex int) float64 {
	queryCounter := makeDomainAnnotationCounterFromDB(queryFilename, queryColumnIndex)
	candidateCounter := makeDomainAnnotationCounterFromDB(candidateFilename, candidateColumnIndex)
	if queryCounter.Unique() == 0 && candidateCounter.Unique() == 0 {
		return 0.0
	}
	// compute intersection
	intersectCounter := queryCounter.Intersect(candidateCounter)
	intersectCardinality := intersectCounter.Unique()
	return float64(intersectCardinality) / float64(queryCounter.Unique()+candidateCounter.Unique()-intersectCardinality)
}

func annotateEntityCounter(counter *counter.Counter) map[string]int {
	elems, freqs := counter.Freqs()
	classes := make(map[string]int)
	for i, entity := range elems {
		for _, c := range entityToClass[strings.ToLower(entity.(string))] {
			if _, ok := classes[c]; !ok {
				classes[c] = freqs[i]
			} else {
				classes[c] = classes[c] + freqs[i]
			}
		}
	}
	return classes
}
func annotateEntityCounterPlus(counter *counter.Counter) map[string]int {
	classes := make(map[string]int)
	elems, freqs := counter.Freqs()
	for i, elem := range elems {
		e := strings.ToLower(elem.(string))
		if len(entityToClass[e]) == 0 {
			log.Printf("No class found for entity %s", e)
		}
		for _, c := range entityToClass[e] {
			if _, ok := classes[c]; !ok {
				classes[c] = freqs[i]
			} else {
				classes[c] = classes[c] + freqs[i]
			}
		}
	}
	return classes
}

func computeEntityConsistencyAndExpansion(queryTable, candidateTable string, queryTextColumns, candidateTextColumns []int) float64 {
	db, err := sql.Open("sqlite3", AnnotationDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// computing w(X,c)
	querySubjectCol := getSubjectColumn(queryTable)
	candidateSubjectCol := getSubjectColumn(candidateTable)
	if querySubjectCol == -1 || candidateSubjectCol == -1 {
		return 0.0
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT class, class_frequncy, num_entities FROM %s WHERE table_name="%s" AND column_index=%d;`, AllAnnotationTable, queryTable, querySubjectCol))
	if err != nil {
		panic(err)
	}
	//
	queryClassScores := make(map[string]float64)
	for rows.Next() {
		var class string
		var freq int
		var entityCard int
		err := rows.Scan(&class, &freq, &entityCard)
		if err != nil {
			panic(err)
		}
		queryClassScores[class] = math.Pow(float64(freq), n1) / math.Pow(float64(entityCard), m1)
	}
	rows.Close()
	// finding Y\X classes
	queryEntityCounter, err := makeDomainEntityCounter(queryTable, querySubjectCol)
	if err != nil {
		return 0.0
	}
	candidateEntityCounter, err := makeDomainEntityCounter(candidateTable, candidateSubjectCol)
	if err != nil {
		return 0.0
	}
	differenceEntityCounter := candidateEntityCounter.Difference(queryEntityCounter)
	// annotate the difference
	candidateAnnotations := annotateEntityCounter(differenceEntityCounter)
	//if candidateEntityCounter.Unique() != differenceEntityCounter.Unique() {
	//	log.Printf("c %d q %d f %d", candidateEntityCounter.Unique(), queryEntityCounter.Unique(), differenceEntityCounter.Unique())
	//}
	candidateClassScores := make(map[string]float64)
	entityCard := candidateEntityCounter.Unique()
	for class, freq := range candidateAnnotations {
		candidateClassScores[class] = math.Pow(float64(freq), n2) / math.Pow(float64(entityCard), m2)
	}
	d1 := queryTable[:strings.LastIndex(queryTable, "____")]
	d2 := candidateTable[:strings.LastIndex(candidateTable, "____")]
	if d1 == d2 {
		log.Printf("candidateEntityCounter: %v", candidateEntityCounter)
		//log.Printf("len(candidateClassScores): %d and candidateEntityCounter.Unique(): %d and differenceEntityCounter.Unique(): %d", len(candidateClassScores), candidateEntityCounter.Unique(), differenceEntityCounter.Unique())
		//log.Printf("len(queryClassScores): %d", len(queryClassScores))
		//if len(candidateClassScores) != 0 && len(queryClassScores) != 0 {
		//	log.Printf("candidateClassScores: %v", candidateClassScores)
		//	log.Printf("queryClassScores: %v", queryClassScores)
		//}
	}
	// computing L(X).L(Y\X)
	SEP := 0.0
	for c, w := range candidateClassScores {
		if s, ok := queryClassScores[c]; ok {
			SEP += s * w
		}
	}
	return SEP
}

func computeSarmaUnionabilityScores(queryFilename, candidateFilename string, queryTextColumns, candidateTextColumns []int) (float64, float64) {
	ece := computeEntityConsistencyAndExpansion(queryFilename, candidateFilename, queryTextColumns, candidateTextColumns)
	sc := 0.0
	if ece > 0.0 {
		sc = computeSchemaConsistency(queryFilename, candidateFilename, queryTextColumns, candidateTextColumns)
	}
	sarma := ece * sc
	d1 := queryFilename[:strings.LastIndex(queryFilename, "____")]
	d2 := candidateFilename[:strings.LastIndex(candidateFilename, "____")]
	if d1 == d2 {
		//if sarma != 0.0 {
		log.Printf("query: %s and candidate: %s has ece: %f * sc: %f = sarma: %f", queryFilename, candidateFilename, ece, sc, sarma)
	}
	return ece, sc
}

func DoFindSarmaUnionableTables(files <-chan string, fanout int) <-chan *sarmaResult {
	out := make(chan *sarmaResult, 10000)
	wg := &sync.WaitGroup{}
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		go func() {
			for queryTable := range files {
				start := GetNow()
				findSarmaUnionableTables(queryTable, out)
				fmt.Printf("Processed the query in %.2f seconds\n", GetNow()-start)
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func DoSaveSarmaScores(scores <-chan *sarmaResult) <-chan ProgressCounter {
	log.Printf("saving sarma scores")
	db, err := sql.Open("sqlite3", SarmaDB)
	if err != nil {
		panic(err)
	}
	// Create table
	_, err = db.Exec(fmt.Sprintf(`drop table if exists %s; create table if not exists %s (query_table text, candidate_table text, entity_consistency_expansion real, schema_consistency real, sarma real);`, SarmaTable, SarmaTable))
	if err != nil {
		panic(err)
	}
	stmt, err := db.Prepare(fmt.Sprintf(`insert into %s(query_table, candidate_table, entity_consistency_expansion, schema_consistency, sarma) values(?, ?, ?, ?, ?);`, SarmaTable))
	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	progress := make(chan ProgressCounter)
	go func() {
		for score := range scores {
			//log.Printf("saving the score of %s", score.queryTable)
			_, err = stmt.Exec(score.queryTable, score.candidateTable, score.entityConsistencyAndExpansion, score.schemaConsistency, score.entityConsistencyAndExpansion*score.schemaConsistency)
			if err != nil {
				panic(err)
			}
			progress <- ProgressCounter{1}
		}
		wg.Done()
	}()
	go func() {
		wg.Wait()
		db.Close()
		close(progress)
	}()
	return progress
}

type sarmaResult struct {
	queryTable                    string
	candidateTable                string
	entityConsistencyAndExpansion float64
	schemaConsistency             float64
}

func findSarmaUnionableTables(queryTable string, out chan *sarmaResult) {
	resultBucket := bucketize(queryTable)
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			for candidateTable := range resultBucket {
				queryTextColumns := getTextDomains(queryTable)
				candidateTextColumns := getTextDomains(candidateTable)
				if len(queryTextColumns) == 0 || len(candidateTextColumns) == 0 {
					continue
				}
				ece, sc := computeSarmaUnionabilityScores(queryTable, candidateTable, queryTextColumns, candidateTextColumns) //, yg.Copy())
				if ece*sc != 0.0 {
					out <- &sarmaResult{
						queryTable:                    queryTable,
						candidateTable:                candidateTable,
						entityConsistencyAndExpansion: ece,
						schemaConsistency:             sc,
					}
				}
			}
			log.Printf("processed the bucket")
			wg.Done()
		}()
	}
	wg.Wait()
	log.Printf("done waiting")
}

func computeValueConsistency(queryTable, candidateTable string, queryColumnIndex, candidateColumnIndex int) float64 {
	valueConsistency := 0.0
	numPairs := 0
	wg := &sync.WaitGroup{}
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			for vp := range makeDomainValuePairs(queryTable, candidateTable, queryColumnIndex, candidateColumnIndex) {
				d := dice(vp.value1, vp.value2)
				valueConsistency += d
				if d > 0.0 {
					numPairs += 1
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if numPairs == 0.0 {
		return 0.0
	}
	return valueConsistency / float64(numPairs)
}

type valuePair struct {
	value1 string
	value2 string
}

func makeDomainValuePairs(queryTable, candidateTable string, queryColumnIndex, candidateColumnIndex int) <-chan valuePair {
	queryValues, err := getDomainValues(queryTable, queryColumnIndex)
	if err != nil {
		panic(err)
	}
	candidateValues, err := getDomainValues(candidateTable, candidateColumnIndex)
	if err != nil {
		panic(err)
	}
	maxPairNum := 4000
	out := make(chan valuePair)
	go func() {
		seenPairs := make(map[valuePair]bool)
		for _, qval := range queryValues {
			for _, cval := range candidateValues {
				p1 := valuePair{
					value1: qval,
					value2: cval,
				}
				p2 := valuePair{
					value1: cval,
					value2: qval,
				}
				if _, ok := seenPairs[p1]; !ok {
					out <- p1
					seenPairs[p1] = true
					seenPairs[p2] = true
				}
			}
			if len(seenPairs) == maxPairNum {
				break
			}
		}
		close(out)
	}()
	return out
}

func getDomainValues(tableID string, columnIndex int) ([]string, error) {
	p := filepath.Join(OutputDir, "domains", tableID, fmt.Sprintf("%d.values", columnIndex))
	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		v := scanner.Text()
		values = append(values, v)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	file.Close()
	return values, nil
}

func tokenizeAnnotation(annotation string) []string {
	var patternSymb *regexp.Regexp
	patternSymb = regexp.MustCompile(`[^a-z ]`)
	annotation = strings.ToLower(annotation)
	words := patternSymb.Split(annotation, -1)
	return words
}

func dice(a, b string) (coefficient float64) {
	a = strings.ToLower(a)
	b = strings.ToLower(b)
	bigrams := map[string]int{}
	denom := 0.0
	for i := 0; i < len(a)-1; i++ {
		bigrams[a[i:i+2]]++
		denom++
	}
	for i := 0; i < len(b)-1; i++ {
		if _, ok := bigrams[b[i:i+2]]; ok {
			coefficient++
		}
		denom++
	}
	return 2 * coefficient / denom
}

func GetOntDomain(yg *yago.Yago, values []string, numHash int, entityClass map[string][]string, transFun func(string) string, tokenFun func(string) []string) ([]uint64, []uint64, []uint64, int, int, int) {
	// The set of entities found
	noAnnotation := make(map[string]bool)
	annotation := make(map[string]bool)
	// Get the unique values
	uniqueValues := unique(values)
	cardinality := len(uniqueValues)
	// Match unique data values with YAGO entities
	for _, value := range uniqueValues {
		foundEntities := yg.MatchEntity(value, 3)
		if len(foundEntities) == 0 {
			noAnnotation[value] = true
		}
		for _, entity := range foundEntities {
			annotation[entity] = true
		}
	}
	entities := make([]string, 0)
	for k, _ := range annotation {
		entities = append(entities, k)
	}
	noEntities := make([]string, 0)
	for k, _ := range noAnnotation {
		noEntities = append(noEntities, k)
	}
	l1_annotations := annotateEntities(entities, entityClass)
	attMinhash := getMinhash(tokenFun, transFun, uniqueValues, numHash)
	ontMinhash := getMinhash(tokenFun, transFun, l1_annotations, numHash)
	//ontMinhash := getMinhash(tokenFun, transFun, annotation, numHash)
	noOntMinhash := getMinhash(tokenFun, transFun, noEntities, numHash)
	return ontMinhash, noOntMinhash, attMinhash, len(l1_annotations), len(noEntities), cardinality
}

func GetOntDomainPlus(values []string, numHash int, lookup map[string]map[string]bool, counts map[string]int, entityClass map[string][]string, transFun func(string) string, tokenFun func(string) []string) ([]uint64, []uint64, []uint64, int, int, int) {
	// The set of entities found
	noAnnotation := make([]string, 0)
	annotation := make([]string, 0)
	// Get the unique values
	uniqueValues := unique(values)
	cardinality := len(uniqueValues)
	// updating domain cardinality
	for _, value := range uniqueValues {
		words := getWords(value)
		foundEntities := findEntities(words, lookup, counts)
		if len(foundEntities) == 0 {
			noAnnotation = append(noAnnotation, value)
		}
		for _, ent := range foundEntities {
			annotation = append(annotation, ent)
		}
	}
	l1_annotations := annotateEntities(annotation, entityClass)
	attMinhash := getMinhash(tokenFun, transFun, uniqueValues, numHash)
	ontMinhash := getMinhash(tokenFun, transFun, l1_annotations, numHash)
	//ontMinhash := getMinhash(tokenFun, transFun, annotation, numHash)
	noOntMinhash := getMinhash(tokenFun, transFun, noAnnotation, numHash)
	return ontMinhash, noOntMinhash, attMinhash, len(l1_annotations), len(noAnnotation), cardinality
}

func annotateEntities(entities []string, entityClass map[string][]string) []string {
	//K := 10
	classes := make(map[string]int)
	annotations := make([]string, 0)
	for _, ent := range entities {
		e := strings.ToLower(ent)
		if len(entityClass[e]) == 0 {
			log.Printf("No class found for entity %s", e)
			//continue
		}
		for _, c := range entityClass[e] {
			if _, ok := classes[c]; !ok {
				classes[c] = 1
				annotations = append(annotations, c)
			}
			//else {
			//	classes[c] = classes[c] + 1
			//}
		}
	}
	/*
		freqs := make([]int, 0)
		freqClass := make(map[int][]string)
		for k, v := range classes {
			if _, ok := freqClass[v]; !ok {
				freqClass[v] = []string{k}
				freqs = append(freqs, v)
			} else {
				freqClass[v] = append(freqClass[v], k)
			}
		}

		sort.Ints(freqs)
		annotations := make([]string, 0)
		for i := len(freqs) - 1; i >= 0; i-- {
			for _, c := range freqClass[freqs[i]] {
				annotations = append(annotations, c)
				if len(annotations) == K {
					return annotations
				}
			}
		}
	*/
	return annotations
}

func getMinhash(tokenFun func(string) []string, transFun func(string) string, column []string, numHash int) []uint64 {
	values := TokenizedValues(column, tokenFun, transFun)
	mh := minhashlsh.NewMinhash(seed, numHash)

	for tokens := range values {
		for _, word := range tokens {
			mh.Push([]byte(word))
		}
	}
	return mh.Signature()
}
