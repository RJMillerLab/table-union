package opendata

import (
	"log"
	"testing"
)

func Test_dice(t *testing.T) {
	s1 := "abc"
	s2 := "abc"
	log.Printf("dice: %f", dice(s1, s2))
	if dice(s1, s2) != 1 {
		t.Fail()
	}
}
