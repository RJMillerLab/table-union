import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial.distance import cosine 
import sqlite3
import collections
import pandas as pd
import os
import json


def get_domain(table_dir, table_id, column_id):
    df = pd.read_csv(os.path.join(table_dir, table_id))
    # the first two lines of csv files are headers and booleans for numerical columns
    col = np.array(df[df.columns[int(column_id)]])[1:]
    set_col = set(col)
    set_col.discard("")
    set_col.discard(np.nan)
    return set_col


def is_equal(dom1, dom2): 
    if dom1 == dom2:
        return True
    else:
        return False

def jaccard_similarity(x,y):
    intersection_cardinality = len(set.intersection(*[x, y]))
    union_cardinality = len(set.union(*[x, y]))
    if union_cardinality > 0:
        jac = intersection_cardinality/float(union_cardinality)
    else:
        # empty domains
        jac = None
    return jac


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataset", default="/home/fnargesian/WIKI_TABLE/search-index.db")
parser.add_argument("-oa", "--outputa", default="jaccard_best_pc.pdf")
parser.add_argument("-ob", "--outputb", default="jaccard_first_pc.pdf")
parser.add_argument("-oc", "--outputc", default="best_pc_jaccard.pdf")
parser.add_argument("-od", "--outputd", default="first_pc_jaccard.pdf")
parser.add_argument("-otd", "--onttabledir", default="/home/fnargesian/WIKI_TABLE/onttables")
parser.add_argument("-oe", "--outpute", default="first_vs_best.pdf")
parser.add_argument("-of", "--outputf", default="best_vs_first.pdf")
parser.add_argument("-td", "--tabledir", default="/home/ekzhu/WIKI_TABLE/tables")
args = parser.parse_args(sys.argv[1:])

seen_pairs = []
pairs = []
pcs_cosines = collections.deque([])
cosines = collections.deque([])
jaccards = collections.deque([])
db = sqlite3.connect(args.dataset)
cursor = db.cursor()
dt = np.dtype(float)
dt = dt.newbyteorder('>')
pos_count = 0
neg_count = 0
# loading domain pairs and jaccard scores
with open('testdata/pairs.json') as fp:
    pairs = json.load(fp)
with open('testdata/jaccards.json') as fp:
    jaccards = json.load(fp)
count = 0
print("Computing cosine...")
for p in pairs:
    count += 1
    if count % 100 == 0:
        print("processed %d pairs." % count)
    first_cos = None
    max_cos = -1.0
    for bin_vec1, pc_index1 in \
        cursor.execute("select vec, pc_index from search_index where table_id=? and column_index=?", (p[0], p[1])).fetchall():
        vec1 = np.frombuffer(bin_vec1, dtype=dt)
        for bin_vec2, pc_index2 in \
            cursor.execute("select vec, pc_index from search_index where table_id=? and column_index=?", (p[2], p[3])).fetchall():
            vec2 = np.frombuffer(bin_vec2, dtype=dt)
            cos = 1.0 - cosine(vec1, vec2)
            if pc_index2 == 0 and pc_index1 == 0:
                first_cos = cos
            if cos > max_cos:
                max_cos = cos
    pcs_cosines.append(max_cos)
    cosines.append(first_cos)

# writing points to json
print("Saving pairs and scores...")
with open('testdata/cosines.json', 'w') as fp:
    json.dump(list(cosines), fp)
with open('testdata/pcs_cosines.json', 'w') as fp:
    json.dump(list(pcs_cosines), fp)
# plotting nearest pc cosines vs jaccards
print("Plotting...")
plt.figure(figsize=(18, 18))
plt.ylabel('nearest pc cosine', fontsize=24)
plt.xlabel('ontology jaccard', fontsize=24)
plt.title('ontology jaccard vs nearest pc cosine of column pairs', fontsize=24)
plt.scatter(jaccards,pcs_cosines)
plt.savefig(args.outputa)
# plotting cosines vs jaccards
plt.figure(figsize=(18, 18))
plt.ylabel('first pc cosine', fontsize=24)
plt.xlabel('ontology jaccard', fontsize=24)
plt.title('ontology jaccard vs first pc cosine of column pairs', fontsize=24)
plt.scatter(jaccards,cosines)
plt.savefig(args.outputb)
# plotting jaccard vs cosine
plt.figure(figsize=(18, 18))
plt.xlabel('nearest pc cosine', fontsize=24)
plt.ylabel('ontology jaccard', fontsize=24)
plt.title('nearest pc cosine vs ontology jaccard of column pairs', fontsize=24)
plt.scatter(pcs_cosines, jaccards)
plt.savefig(args.outputc)
# 
plt.figure(figsize=(18, 18))
plt.xlabel('nearest pc cosine', fontsize=24)
plt.ylabel('ontology jaccard', fontsize=24)
plt.title('first pc cosine vs ontology jaccard of column pairs', fontsize=24)
plt.scatter(cosines, jaccards)
plt.savefig(args.outputd)
# 
plt.figure(figsize=(18, 18))
plt.xlabel('nearest pc cosine', fontsize=24)
plt.ylabel('best pc cosine', fontsize=24)
plt.title('nearest pc cosine vs best pc cosine (number of pcs = 3) of column pairs', fontsize=24)
plt.scatter(pcs_cosines, cosines)
plt.savefig(args.outpute)
# 
plt.figure(figsize=(18, 18))
plt.ylabel('nearest pc cosine', fontsize=24)
plt.xlabel('best pc cosine', fontsize=24)
plt.title('best pc cosine (number of pcs = 3) vs nearest pc cosine of column pairs', fontsize=24)
plt.scatter(cosines, pcs_cosines)
plt.savefig(args.outputf)
print("Number of processed pairs: %d" % count)
print("Done.")
