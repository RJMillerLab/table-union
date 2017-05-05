import sqlite3
import itertools
import time

def parse_yago_type(parts):
    if len(parts) == 4:
        id = parts[0].strip("<>")
        entity = parts[1].strip("<>")
        category = parts[3].strip("<>")
        return id, entity, category
    else:
        return None

def read_yago_types(filename):
    f = open(filename, "r")
    for line in f:
        line = line.strip()
        parts = line.split("\t")
        t = parse_yago_type(parts)
        if t: yield t
    f.close()

def read_yago_taxonomy(filename):
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            parts = line.split("\t")
            if line.startswith("<id_"):
                subcat = parts[1].strip("<>")
                supcat = parts[3].strip("<>")
                yield (subcat, supcat)

def init_db(dbname):
    db = sqlite3.connect(dbname)
    c = db.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS types (
        id CHAR(100),
        entity STRING,
        category STRING
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS taxonomy (
        subcategory STRING,
        supcategory STRING
    )
    """)
    db.commit()
    return db

def import_data(db, table, n, rows):
    start = time.time()
    cursor = db.cursor()
    marks = ",".join("?" for i in range(n))
    sql = "INSERT INTO %s VALUES (%s)" % (table, marks)

    for i, row in enumerate(rows):
        cursor.execute(sql, row)
        if i % 10000 == 0:
            duration = time.time() - start
            eta = 16927021.0 / (i+1) * duration
            print("Imported %d rows in %.2f seconds, eta = %.2f" % (
                i+1, duration, eta))
            db.commit()

def import_yago_types(types, db):
    return import_data(db, "types", 3, types)

def import_yago_taxonomy(taxonomy, db):
    return import_data(db, "taxonomy", 2, taxonomy)

YAGO_TYPES = "/home/fnargesian/YAGO/yagoTypes.tsv"
YAGO_TAXO  = "/home/fnargesian/YAGO/yagoTaxonomy.tsv"
DB_NAME = "./yago.sqlite3"

types = read_yago_types(YAGO_TYPES)
taxonomy = read_yago_taxonomy(YAGO_TAXO)
db = init_db(DB_NAME)
# import_yago_types(types, db)
import_yago_taxonomy(taxonomy, db)
