import sqlite3
import sys
import os
import csv
import numpy as np
import math

REPO = "/home/fnargesian/WIKI_TABLE/onttables"
DB_NAME = "./yago.sqlite3"

def get_db():
    if os.path.exists(DB_NAME):
        return sqlite3.connect(DB_NAME)

def get_domain(relname, colindex):
    domain = []
    with open(os.path.join(REPO, relname), "r") as f:
        rdr = csv.reader(f)
        next(rdr)
        next(rdr)
        for row in rdr:
            domain.append(row[colindex])
    return domain

def in_domain(attrname, domain):
    values = list(set(domain))
    return "%s IN (%s) " % (attrname, ",".join("?" for i in values)), values

def get_types(db, domain):
    "Get the immediate type of the values"
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
    "Get the level-2 categories"
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

def merge_hist(h1, h2):
    "Merges the histograms into one"
    d = dict(h1)
    for k,v in h2:
        if k in d:
            d[k] += v
        else:
            d[k] = v
    return d.items()

def get_entropy(hist):
    values = [x[0] for x in hist]
    counts = np.array([x[1] for x in hist])
    prob = counts / sum(counts)
    p = prob[prob > 0]
    normalized_ent = -np.sum(np.log(p) * p)

    return normalized_ent

def get_normalized_entropy(hist):
    return get_entropy(hist) / np.log(len(hist))

def normalized_entropy_change(h1, h2):
    h = merge_hist(h1, h2)

    e1 = get_entropy(h1)
    e  = get_entropy(h)

    return (e - e1) / np.log(len(h))

def entropy_change(h1, h2):
    e1 = get_entropy(h1)
    e2 = get_entropy(merge_hist(h1, h2))
    return e2 - e1


# ============== "Finding Related Tables" ==================

def sarma_sim(db, dom1, dom2):
    n = 2
    m = 1
    dom_diff = list(set(dom2).difference(dom1))

    h1 = dict(get_cat2(db, dom1))
    h2 = dict(get_cat2(db, dom_diff))

    h = dict()
    dotprod = 0

    for k, v in h1.items():
        h[k] = float(h1[k]) / len(dom1)

    for k, v in h2.items():
        if k in h:
            dotprod +=  h[k] * math.pow(float(h2[k]), n) / math.pow(len(dom_diff), m)

    return dotprod

def jaccard_sim(db, dom1, dom2):
    labels1 = set([x[0] for x in get_cat2(db, dom1)])
    labels2 = set([x[0] for x in get_cat2(db, dom2)])
    return len(labels1.intersection(labels2))
