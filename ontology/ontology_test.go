package ontology

import "testing"

func TestNewOntology(t *testing.T) {
	rawTypesFile := "/home/fnargesian/YAGO/yagoTypes.tsv"
	rawTaxonomyFile := "/home/fnargesian/YAGO/yagoTaxonomy.tsv"
	typesFile := "types.yago"
	taxonomyFile := "taxonomy.yago"

	// create new taxonomy
	o := NewOntology(rawTypesFile, rawTaxonomyFile, typesFile, taxonomyFile)
	if len(o.EntityType) == 0 {
		t.Fail()
	}
	es := len(o.EntityType)
	if len(o.Taxonomy) == 0 {
		t.Fail()
	}
	ts := len(o.Taxonomy)
	// load taxonomy from backup
	o = LoadOntology(typesFile, taxonomyFile)
	if len(o.EntityType) != es {
		t.Fail()
	}
	if len(o.Taxonomy) != ts {
		t.Fail()
	}
}
