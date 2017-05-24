package embedding

import "testing"

func Test_InMemFastText(t *testing.T) {
	ft, err := InitInMemoryFastText("./fasttext-small.db", func(v string) []string {
		return []string{v}
	}, func(v string) string {
		return v
	})
	if err != nil {
		t.Error(err)
	}
	defer ft.Close()

	if _, err := ft.GetEmb("when"); err != nil {
		t.Error(err)
	}

	values := []string{"time", "team", "united"}
	freqs := []int{1, 10, 5}
	if vec, err := ft.GetDomainEmbSum(values, freqs); err != nil {
		t.Error(err)
	} else {
		t.Log(vec)
	}
}
