import sqlite3
import sys
import os
import csv
from scipy import stats
import numpy as np 


REPO = "/home/fnargesian/WIKI_TABLE/onttables"
DB_NAME = "./yago.sqlite3"

if len(sys.argv) != 5:
    print("Usage: <table1> <column-index1> <table2> <column-index2>>")
    sys.exit()

relname1 = sys.argv[1]
colindex1 = int(sys.argv[2])
relname2 = sys.argv[3]
colindex2 = int(sys.argv[4])

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

def entropy(ont_freq):
    ontvec = [t[1] for t in ont_freq]
    norm_ontvec = [float(f)/sum(ontvec) for f in ontvec]
    return stats.entropy(norm_ontvec,None)

def kl_divergence(ont_freq1, ont_freq2):
    cs = list(set([t[0] for t in ont_freq1] + [t[0] for t in ont_freq2]))
    ontvec1 = [0] * len(cs)
    ontvec2 = [0] * len(cs)
    for t in ont_freq1:
        ontvec1[cs.index(t[0])] = t[1]
    for t in ont_freq2:
        ontvec2[cs.index(t[0])] = t[1]
    norm_ontvec1 = np.asarray([float(f)/sum(ontvec1) for f in ontvec1], dtype=np.float)
    norm_ontvec2 = np.asarray([float(f)/sum(ontvec2) for f in ontvec2], dtype=np.float)
    return np.sum(np.where(norm_ontvec1 != 0, norm_ontvec1 * np.log(norm_ontvec1 / norm_ontvec2), 0))
    
db = sqlite3.connect(DB_NAME)
domain1 = list(get_domain(relname1, colindex1))
domain2 = list(get_domain(relname2, colindex2))
print("domain1 (%d)" % len(domain1))
print("domain2 (%d)" % len(domain2))
print("entropy of domain1: %f" % entropy(get_types(db, domain1)))
print("entropy of domain2: %f" % entropy(get_types(db, domain2)))
print("KL-divergence (type): %f" % kl_divergence(get_types(db, domain1),get_types(db, domain2)))
print("KL-divergence (cat2): %f" % kl_divergence(get_cat2(db, domain1),get_cat2(db, domain2)))

