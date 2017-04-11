package ontology

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"strings"
)

type Ontology struct {
	EntityType map[string][]string
	Taxonomy   map[string][]string
}

func NewOntology(rawTypesFile, rawTaxonomyFile, typesFile, taxonomyFile string) *Ontology {
	es := readEntityType(rawTypesFile)
	err := dumpJson(typesFile, &es)
	if err != nil {
		log.Fatal(err)
	}
	ts := readTaxonomy(rawTaxonomyFile)
	err = dumpJson(taxonomyFile, &ts)
	if err != nil {
		log.Fatal(err)
	}
	return &Ontology{
		EntityType: es,
		Taxonomy:   ts,
	}
}

func LoadOntology(entitiesFile, taxonomyFile string) *Ontology {
	es := make(map[string][]string)
	err := loadJson(entitiesFile, &es)
	if err != nil {
		log.Fatal(err)
	}
	ts := make(map[string][]string)
	err = loadJson(taxonomyFile, &ts)
	if err != nil {
		log.Fatal(err)
	}
	return &Ontology{
		EntityType: es,
		Taxonomy:   ts,
	}
}

func readEntityType(typesFile string) map[string][]string {
	entities := make(map[string][]string)
	// skipping the comment line in YAGO tsv file
	lines, _ := readLines(typesFile)
	lines = lines[1:]
	// line example: <id_nu6jdt_88c_8g5qms>  <A1086_road>    rdf:type        <wikicat_Roads_in_England>
	for _, line := range lines {
		parts := strings.Fields(strings.ToLower(line))
		e := strings.Replace(strings.Replace(parts[1], "<", "", -1), ">", "", -1)
		t := strings.Replace(strings.Replace(parts[3], "<", "", -1), ">", "", -1)
		if _, ok := entities[e]; !ok {
			entities[e] = []string{t}
		} else {
			entities[e] = append(entities[e], t)
		}
	}
	return entities
}

func readTaxonomy(taxonomyFile string) map[string][]string {
	taxonomy := make(map[string][]string)
	// skipping the comment line in YAGO tsv file
	lines, _ := readLines(taxonomyFile)
	lines = lines[1:]
	// line example: <id_1wfbzo7_1m6_1k87a1> <wordnet_agape_101028534>       rdfs:subClassOf <wordnet_religious_ceremony_101028082>
	change := true
	for change {
		change = false
		for _, line := range lines {
			parts := strings.Fields(strings.ToLower(line))
			ch := strings.Replace(strings.Replace(parts[1], "<", "", -1), ">", "", -1)
			p := strings.Replace(strings.Replace(parts[3], "<", "", -1), ">", "", -1)
			if _, ok := taxonomy[ch]; !ok {
				change = true
				taxonomy[ch] = []string{p}
			} else {
				if contains(taxonomy[ch], p) != true {
					change = true
					taxonomy[ch] = append(taxonomy[ch], p)
				}
			}
		}
	}
	return taxonomy
}

func loadJson(file string, v interface{}) error {
	buffer, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	err = json.Unmarshal(buffer, v)
	if err != nil {
		return err
	}
	return nil
}

func dumpJson(file string, v interface{}) error {
	buffer, err := json.Marshal(v)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(file, buffer, 0664)
	if err != nil {
		return err
	}
	return nil
}

func readLines(filename string) ([]string, error) {
	var lines []string
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return lines, err

	}
	buf := bytes.NewBuffer(file)
	for {
		line, err := buf.ReadString('\n')
		if len(line) == 0 {
			if err != nil {
				if err == io.EOF {
					break
				}
				return lines, err
			}
		}
		lines = append(lines, line)
		if err != nil && err != io.EOF {
			return lines, err
		}
	}
	return lines, nil
}

func contains(arr []string, val string) bool {
	for _, a := range arr {
		if a == val {
			return true
		}
	}
	return false
}