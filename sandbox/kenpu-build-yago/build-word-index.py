import sqlite3
import re
import sys
import os
import time

try:
    dbname = sys.argv[1]
    if not os.path.exists(dbname):
        raise(Exception("DB not found"))
except Exception as e:
    print(e)
    sys.exit()

goner = re.compile(r'\([^)]*\)')
nonchar = re.compile(r'[^a-z0-9]')

def clean_word(w):
    w = nonchar.sub("", w)
    if len(w) >= 3:
        return w
    else:
        return None

def get_words(ent):
    words = goner.sub("", ent).replace('_', ' ').lower().split()
    return list(filter(None, map(clean_word, words)))

db = sqlite3.connect(dbname)
c1 = db.cursor()
c1.execute("DROP TABLE IF EXISTS words")
c1.execute("""
        CREATE TABLE words (
            entity text,
            word char(30),
            total integer
        )
        """)
db.commit()

c1.execute("select distinct entity from types")

start = time.time()
c2 = db.cursor()
i = 0
while True:
    row = c1.fetchone()
    if row is None:
        break

    i += 1
    ent = str(row[0])
    words = get_words(ent)
    n = len(words)
    for w in words:
        c2.execute("insert into words values(?,?,?)", (ent, w, n))

    if i % 100000 == 0:
        print("Done %s rows in %.2f seconds" % (i, time.time() - start))
        db.commit()
db.commit()

print("Creating index...")
c1.execute('create index word_idx on words(word)')

db.close()

