package opendata

/*
func Test_MinhashSerialization(t *testing.T) {
	seed = 1
	numHash = 256
	mh := minhashlsh.NewMinhash(seed, numHash)
	words := []string{"hello", "world", "minhas"}
	for _, word := range words {
		mh.Push([]byte(word))
	}
	sig1 := mh.Signature()
	err := writeMinhashSignature(mh, "words.minhash")
	if err != nil {
		log.Printf("error in writing sig to the disk.")
		t.Fail()
	}
	sig2, err := readMinhashSignature("words.minhash", numHash)
	if err != nil {
		log.Printf("error in reading sig from the disk.")
		t.Fail()
	}
	if len(sig2) != 256 {
		log.Printf("wrong length of sig")
		t.Fail()
	}
	for i := range sig1 {
		if sig1[i] != sig2[i] {
			log.Printf("wrong sig")
			t.Fail()
		}
	}
}
*/
