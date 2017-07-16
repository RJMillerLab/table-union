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
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/RJMillerLab/table-union/minhashlsh"
	"github.com/RJMillerLab/table-union/pqueue"
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
	prepareDB()
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

func getSubjectColumn(tableName string) int {
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
	filepath := path.Join(OutputDir, "domains", file, fmt.Sprintf("%d.%s", index, ext))
	f, err := os.Open(filepath)
	if err != nil {
		return
		//out <- &domainAnnotation{
		//	filename: file,
		//	index:    index,
		//}
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

func saveAnnotation(annotation *domainAnnotation, progress chan ProgressCounter) {
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		annotation.saveToDB()
		wg.Done()
	}()
	go func() {
		annotation.saveToFile()
		wg.Done()
	}()
	go func() {
		wg.Wait()
		progress <- ProgressCounter{1}
	}()
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
		//defer db.Close()
		//rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT a2.table_name as candidate_table FROM %s a1, %s a2 WHERE a1.table_name="%s" AND (a1.column_name=a2.column_name OR a1.class=a2.class);`, AllAnnotationTable, AllAnnotationTable, queryFilename))
		rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT a2.table_name as candidate_table FROM %s a2, (SELECT * FROM %s WHERE table_name="%s" ORDER BY class_frequncy limit 20) a1 WHERE a2.subject_column=a2.column_index and (a1.column_name=a2.column_name OR a1.class=a2.class);`, AllAnnotationTable, AllAnnotationTable, queryFilename))
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
		//wg.Done()
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
	queryEntities := readEntities(queryFilename, queryTextColumns[0])
	candidateEntities := readEntities(candidateFilename, candidateTextColumns[0])
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
	rows, err := db.Query(fmt.Sprintf(`SELECT column_name, class FROM %s WHERE table_name = "%s" AND        column_index = %d ORDER BY class_frequncy LIMIT 20;`, AllAnnotationTable, tableName, columnIndex))
	if err != nil {
		panic(err)
	}
	//
	domainCounter := counter.NewCounter()
	for rows.Next() {
		var anno string
		var columnName string
		err := rows.Scan(&columnName, &anno)
		domainCounter.Update(columnName)
		words := tokenizeAnnotation(anno)
		if err != nil {
			panic(err)
		}
		for word := range words {
			domainCounter.Update(word)
		}
	}
	rows.Close()
	return domainCounter
}

func makeDomainEntityCounter(tableName string, columnIndex int) (*counter.Counter, error) {
	filepath := path.Join(OutputDir, "domains", tableName, fmt.Sprintf("%d.entities", columnIndex))
	f, err := os.Open(filepath)
	if err != nil {
		//log.Printf("error loading %s", filepath)
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
	//rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT class, class_frequncy, num_entities FROM %s WHERE table_name="%s";`, SubjectAnnotationTable, queryTable))
	querySubjectCol := getSubjectColumn(queryTable)
	candidateSubjectCol := getSubjectColumn(candidateTable)
	if querySubjectCol == -1 || candidateSubjectCol == -1 {
		return 0.0
	}
	//rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT class, class_frequncy, num_entities FROM %s WHERE table_name="%s";`, AllAnnotationTable, queryTable))
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
	//queryEntityCounter, err := makeDomainEntityCounter(queryTable, queryTextColumns[0])
	queryEntityCounter, err := makeDomainEntityCounter(queryTable, querySubjectCol)
	if err != nil {
		return 0.0
	}
	//candidateEntityCounter, err := makeDomainEntityCounter(candidateTable, candidateTextColumns[0])
	candidateEntityCounter, err := makeDomainEntityCounter(candidateTable, candidateSubjectCol)
	if err != nil {
		return 0.0
	}
	differenceEntityCounter := candidateEntityCounter.Difference(queryEntityCounter)
	// annotate the difference
	candidateAnnotations := annotateEntityCounter(differenceEntityCounter)
	candidateClassScores := make(map[string]float64)
	entityCard := candidateEntityCounter.Unique()
	for class, freq := range candidateAnnotations {
		candidateClassScores[class] = math.Pow(float64(freq), n2) / math.Pow(float64(entityCard), m2)
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
	//return 1.0, 1.0
	ece := computeEntityConsistencyAndExpansion(queryFilename, candidateFilename, queryTextColumns, candidateTextColumns)
	sc := 0.0
	if ece > 0.0 {
		log.Printf("non negative entity criterion, computing schema consistency!")
		sc = computeSchemaConsistency(queryFilename, candidateFilename, queryTextColumns, candidateTextColumns)
	}
	sarma := ece * sc
	//if sarma != 0.0 {
	log.Printf("query: %s and candidate: %s has ece: %f * sc: %f = sarma: %f", queryFilename, candidateFilename, ece, sc, sarma)
	//}
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
			log.Printf("score is : %f", score.entityConsistencyAndExpansion)
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
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			for candidateTable := range resultBucket {
				log.Printf("cand: %s and query: %s", candidateTable, queryTable)
				queryTextColumns := getTextDomains(queryTable)
				candidateTextColumns := getTextDomains(candidateTable)
				//log.Printf("len(queryTextColumns): %d and len(candidateTextColumns): %d", len(queryTextColumns), len(candidateTextColumns))
				if len(queryTextColumns) == 0 || len(candidateTextColumns) == 0 {
					continue
				}
				ece, sc := computeSarmaUnionabilityScores(queryTable, candidateTable, queryTextColumns, candidateTextColumns)
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

func GetOntDomain(values []string, numHash int, lookup map[string]map[string]bool, counts map[string]int, entityClass map[string][]string, transFun func(string) string, tokenFun func(string) []string) ([]uint64, []uint64, []uint64, int, int, int) {
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
	//l1_annotations := annotateEntities(annotation, entityClass)
	attMinhash := getMinhash(tokenFun, transFun, uniqueValues, numHash)
	//ontMinhash := getMinhash(tokenFun, transFun, l1_annotations, numHash)
	ontMinhash := getMinhash(tokenFun, transFun, annotation, numHash)
	//ontMinhash := getMinhash(tokenFun, transFun, annotation, numHash)
	noOntMinhash := getMinhash(tokenFun, transFun, noAnnotation, numHash)
	return ontMinhash, noOntMinhash, attMinhash, len(annotation), len(noAnnotation), cardinality
}

func annotateEntities(entities []string, entityClass map[string][]string) []string {
	K := 10
	classes := make(map[string]int, 0)
	for _, ent := range entities {
		e := strings.ToLower(ent)
		if len(entityClass[e]) == 0 {
			log.Printf("No class found for entity %s", e)
			//continue
		}
		for _, c := range entityClass[e] {
			if _, ok := classes[c]; !ok {
				classes[c] = 1
			} else {
				classes[c] = classes[c] + 1
			}
		}
	}
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
