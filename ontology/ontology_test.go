package ontology

import (
	"log"
	"testing"
)

func TestNewOntology(t *testing.T) {
	rawTypesFile := "/home/fnargesian/YAGO/yagoTypes.tsv"
	rawTaxonomyFile := "/home/fnargesian/YAGO/yagoTaxonomy.tsv"
	typesFile := "types.yago"
	taxonomyFile := "taxonomy.yago"
	flatTaxonomyFile := "flat_taxonomy.yago"
	// create new taxonomy
	o := NewOntology(rawTypesFile, rawTaxonomyFile, typesFile, taxonomyFile, flatTaxonomyFile)
	if len(o.EntityType) == 0 {
		t.Fail()
	}
	es := len(o.EntityType)
	if len(o.Taxonomy) == 0 {
		t.Fail()
	}
	ts := len(o.Taxonomy)
	if len(o.FlatTaxonomy) == 0 {
		t.Fail()
	}
	fts := len(o.FlatTaxonomy)
	// load taxonomy from backup
	o = LoadOntology(typesFile, taxonomyFile, flatTaxonomyFile)
	if len(o.EntityType) != es {
		log.Printf("error in reading entity types: %d vs %d", len(o.EntityType), es)
		t.Fail()
	}
	if len(o.Taxonomy) != ts {
		log.Printf("error in reading taxonomy: %d vs %d", len(o.Taxonomy), ts)
		t.Fail()
	}
	if len(o.FlatTaxonomy) != fts {
		log.Printf("error in reading flat taxonomy: %d vs %d", len(o.FlatTaxonomy), fts)
		t.Fail()
	}
	// create ontology map
	ancestorsFile := "ancestors.yago"
	ancestors := o.entityToAncestors(ancestorsFile)
	if len(ancestors) == 0 {
		t.Fail()
	}
}
