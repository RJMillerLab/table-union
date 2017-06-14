package simhashlsh

import "math/rand"

type signature []uint8

// Represents a SinHash signature - an array of hash values
type simhash struct {
	sig signature
}

//signature generates the simhash of an attribute (a bag of values) using the hyperplanes
// Initialize a MinHash object with a seed and the number of
// hash functions.
func newSimhash(hs hyperplanes, e []float64) *simhash {
	sig := newSignature(hs, e)
	return &simhash{
		sig: sig,
	}
}

func newSignature(hyperplanes hyperplanes, e []float64) signature {
	sigarr := make([]uint8, len(hyperplanes))
	for hix, h := range hyperplanes {
		var dp float64
		for k, v := range e {
			dp += h[k] * float64(v)
		}
		if dp >= 0 {
			sigarr[hix] = uint8(1)
		} else {
			sigarr[hix] = uint8(0)
		}
	}
	return sigarr
}

// the hyperplanes
type hyperplanes [][]float64

//NewHyperplanes generates and initializes a set of d hyperplanes with s dimensions.
func NewHyperplanes(d, s int) hyperplanes {
	hs := make([][]float64, d)
	for i := 0; i < d; i++ {
		v := make([]float64, s)
		for i := 0; i < s; i++ {
			n := rand.NormFloat64()
			v[i] = n
		}
		hs[i] = v
	}
	return hs
	//return generateHPs(d, s)
}
