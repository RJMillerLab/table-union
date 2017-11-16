package benchmarkserver

import (
	"log"
	"testing"

	"github.com/gonum/floats"
)

//func Test_combination(t *testing.T) {
//	n := 7
//	k := 5
//	log.Printf("c: %d", combination(n, k))
//}

func Test_hyoperGeometricProb(t *testing.T) {
	//log.Printf("sd: %f", sameDomainProb(0.001, 900, 1000))
	//log.Printf("sd: %f", sameDomainProb(0.3, 900, 1000))
	//log.Printf("sd: %f", sameDomainProb(0.5, 900, 1000))
	//log.Printf("sd: %f", sameDomainProb(0.7, 900, 1000))
	log.Printf("sd: %f", sameDomainProb(0.0, 5, 5))
	log.Printf("sd: %f", sameDomainProb(1.0, 5, 5))
}

func Test_sameDomainProb(t *testing.T) {
	j := 0.2857 //0.027344 //1.0
	nA := 100   //86      //4
	nB := 80    //270     //6
	log.Printf("same domain prob: %f", sameDomainProb(j, nA, nB))
	//if sameDomainProb(j, nA, nB) > 1.0 {
	//	t.Fail()
	//}
	j = 0.2  //1.0
	nA = 20  //4
	nB = 160 //6
	log.Printf("same domain prob: %f", sameDomainProb(j, nA, nB))
}

func Test_getHotellingScore(t *testing.T) {
	m1 := []float64{38.69, 68.45, 18.3}
	//m2 := []float64{39.44444, 62.27778, 20.11111}
	m2 := m1
	cv1 := []float64{1.256737, -9.94263, 3.266316}
	//cv2 := []float64{1.496732, -10.4307, 0.647712}
	cv2 := cv1
	//cv1 := []float64{1.256737, -9.94263, 3.266316,
	//	-9.94263, 169.3132, -38.3,
	//	3.266316, -38.3, 31.8}
	//cv2 := []float64{
	//	1.496732, -10.4307, 0.647712,
	//	-10.4307, 175.1536, -28.5621,
	//	0.647712, -28.5621, 24.45752}
	card1 := 20
	card2 := 18
	ht2, f := getT2Statistics(m1, m2, cv1, cv2, card1, card2)
	log.Printf("T2: %f, f: %f", ht2, f)
}

func Test_sortFloats(t *testing.T) {
	a := []float64{0.3, 0.4, 0.1, 0.6, 0.8}
	log.Printf("a: %v", a)
	c := make([]float64, len(a))
	copy(c, a)
	log.Printf("c: %v", c)
	s := c
	inds := make([]int, len(s))
	log.Printf("s: %v", s)
	floats.Argsort(s, inds)
	log.Printf("----------")
	log.Printf("a: %v", a)
	log.Printf("c: %v", c)
	log.Printf("s: %v", s)
	log.Printf("inds: %v", inds)
}
