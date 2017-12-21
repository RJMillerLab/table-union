package opendata

import (
	"log"
	"testing"
)

func Test_SortPercentiles(t *testing.T) {
	ps := make([]Percentile, 0)
	p := Percentile{
		Value:      0.994959899197984,
		ValuePlus:  0.994959899197984,
		ValueMinus: 0.9824996499929999,
	}
	ps = append(ps, p)
	//
	p = Percentile{
		Value:      0.9986555525678946,
		ValuePlus:  0.9989244420543156,
		ValueMinus: 0.9943533207851573,
	}
	ps = append(ps, p)
	p = Percentile{
		Value:      0.9983445777111444,
		ValuePlus:  0.9992503748125937,
		ValueMinus: 0.9971889055472264,
	}
	ps = append(ps, p)
	//
	p = Percentile{
		Value:      0.999988,
		ValueMinus: 0.709250,
		ValuePlus:  0.999999,
	}
	ps = append(ps, p)
	//
	log.Printf("ps before: %v", ps)
	sps, i := SortPercentiles(ps)
	log.Printf("sps: %v", sps)
	log.Printf("best: %d", i)
	log.Printf("ps: %v", ps)
}
