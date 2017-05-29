package embserver

import "testing"

func Test_Jaccard(t *testing.T) {
	d1 := []string{"a", "a", "a", "b"}
	d2 := []string{"B", "c", "c", "d"}
	if Jaccard(d1, d2) != 0.25 {
		t.Fail()
	}
}

func Test_Containment(t *testing.T) {
	d1 := []string{"a", "a", "a", "b"}
	d2 := []string{"B", "c", "c", "d"}
	if Containment(d1, d2) != 0.5 {
		t.Fail()
	}
}
