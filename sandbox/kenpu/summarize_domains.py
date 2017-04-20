import sqlite3
import sys
import os
import csv

REPO = "/home/fnargesian/WIKI_TABLE/onttables"
DB_NAME = "./yago.sqlite3"

if len(sys.argv) != 3:
    print("Usage: <table> <column-index>>")
    sys.exit()

relname = sys.argv[1]
colindex = int(sys.argv[2])

def get_domain(relname, colindex):
    with open(os.path.join(REPO, relname), "r") as f:
        rdr = csv.reader(f)
        next(rdr)
        next(rdr)
        for row in rdr:
            yield row[colindex]

def in_domain(attrname, domain):
    values = list(set(domain))
    return "%s IN (%s) " % (attrname, ",".join("?" for i in values)), values

def get_types(db, domain):
    c = db.cursor()
    in_clause, values = in_domain("entity", domain)
    sql = """
    SELECT category, count(*) as freq
    FROM types
    WHERE %s
    GROUP BY category
    ORDER BY freq
    """ % in_clause
    c.execute(sql, values)
    result = c.fetchall()
    c.close()
    return result

def get_cat2(db, domain):
    c = db.cursor()
    in_clause, values = in_domain("entity", domain)

    sql = """
    SELECT supcategory, count(*) as freq
    FROM types JOIN taxonomy ON types.category = taxonomy.subcategory
    WHERE %s
    GROUP BY supcategory
    ORDER BY freq
    """ % in_clause

    c.execute(sql, values)
    result = c.fetchall()
    c.close()
    return result

db = sqlite3.connect(DB_NAME)
domain = list(get_domain(relname, colindex))
print("domain (%d)" % len(domain))
print("\n".join(domain))
print("========================")
print("\n".join(str(x) for x in get_types(db, domain)))
print("========================")
print("\n".join(str(x) for x in get_cat2(db, domain)))

