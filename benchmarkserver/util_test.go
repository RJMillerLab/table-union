package benchmarkserver

import (
	"log"
	"testing"
)

//func Test_combination(t *testing.T) {
//	n := 7
//	k := 5
//	log.Printf("c: %d", combination(n, k))
//}

func Test_hyoperGeometricProb(t *testing.T) {
	N := 120000
	K := 4
	n := 3
	k := 2
	log.Printf("%f", hyperGeometricProb(k, K, n, N))
}

func Test_sameDomainProb(t *testing.T) {
	j := 0.027344 //1.0
	nA := 86      //4
	nB := 135     //6
	if sameDomainProb(j, nA, nB) > 1.0 {
		t.Fail()
	}
}

/*
func Test_getHotellingScore(t *testing.T) {
	//m1, m2 []float64, cv1, cv2 []float64, card1, card2
	m1 := []float64{38.69, 68.45, 18.3}
	m2 := []float64{39.44444, 62.27778, 20.11111}
	cv1 := []float64{1.256737, -9.94263, 3.266316,
		-9.94263, 169.3132, -38.3,
		3.266316, -38.3, 31.8}
	cv2 := []float64{
		1.496732, -10.4307, 0.647712,
		-10.4307, 175.1536, -28.5621,
		0.647712, -28.5621, 24.45752}
	card1 := 20
	card2 := 18
	h := getHotellingScore(m1, m2, cv1, cv2, card1, card2)
	log.Printf("%f", h)
}
*/
