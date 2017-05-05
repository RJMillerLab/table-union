package od

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func cleanEntityName(ent string) string {
	x := strings.Replace(strings.ToLower(ent), "_", " ", -1)
	i := strings.Index(x, "(")
	if i > 0 {
		x = x[0:i]
	}
	return strings.TrimSpace(x)
}

type EntityDb struct {
	entityMap *map[string]map[string]bool
}

func NewEntityDb() *EntityDb {
	db, err := sql.Open("sqlite3", yago_db)
	if err != nil {
		panic(err)
	}

	defer db.Close()

	rows, err := db.Query(`
		SELECT entity, category
		FROM types
	`)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	var m = make(map[string]map[string]bool)

	total := 16920001
	counter := 0
	s := GetNow()
	for rows.Next() {
		var ent string
		var cat string
		err = rows.Scan(&ent, &cat)
		if err != nil {
			panic(err)
		}

		ent = cleanEntityName(ent)

		if _, ok := m[ent]; !ok {
			m[ent] = make(map[string]bool)
		}
		m[ent][cat] = true
		counter += 1
		if counter%1000 == 0 {
			fmt.Printf("[entitydb] %d/%d rows in %.2f seconds\n", counter, total, GetNow()-s)
		}
	}

	return &EntityDb{&m}
}

func (edb *EntityDb) GetTypes(value string, m map[string]bool) int {
	return 0
}
