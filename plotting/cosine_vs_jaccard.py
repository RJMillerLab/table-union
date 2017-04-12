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
parser.add_argument("-oa", "--outputa", default="nearest_pc_cosine_vs_ontology_jaccard.pdf")
parser.add_argument("-ob", "--outputb", default="first_pc_cosine_vs_ontology_jaccard.pdf")
parser.add_argument("-od", "--onttabledir", default="/home/fnargesian/WIKI_TABLE/onttables")
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

# generating domain pairs 
print("Computing jaccard...")
for table_id1, column_index1, table_id2, column_index2 in \
        cursor.execute("select distinct t1.table_id as table_id1, t1.column_index as column_id1, t2.table_id as table_id2, t2.column_index as column_index2 from (select * from search_index order by random() limit 1000) t1, (select * from search_index order by random() limit 1000) t2 where t1.table_id!=t2.table_id;").fetchall():
    if (table_id1, column_index1, table_id2, column_index2) not in seen_pairs and \
            (table_id2, column_index2, table_id1, column_index1) not in seen_pairs:
        raw_vec1 = get_domain(args.onttabledir, table_id1, column_index1)
        raw_vec2 = get_domain(args.onttabledir, table_id2, column_index2)
        jac = jaccard_similarity(raw_vec1, raw_vec2)
        pairs.append((table_id1, column_index1, table_id2, column_index2))
        if jac != 0.0:
            pos_count += 1
        jaccards.append(jac)
        seen_pairs.append((table_id1, column_index1, table_id2, column_index2))

print("Computing cosine...")
for p in pairs:
    first_cos = None
    max_cos = -1.0
    for bin_vec1 in \
        cursor.execute("select vec from search_index where table_id=? and column_index=?", (p[0], p[1])).fetchall():
        vec1 = np.frombuffer(bin_vec1[0], dtype=dt)
        for bin_vec2 in \
            cursor.execute("select vec from search_index where table_id=? and column_index=?", (p[2], p[3])).fetchall():
            vec2 = np.frombuffer(bin_vec2[0], dtype=dt)
            cos = 1.0 - cosine(vec1, vec2)
            if first_cos is None:
                first_cos = cos
            if cos > max_cos:
                max_cos = cos
    pcs_cosines.append(max_cos)
    cosines.append(first_cos)

print("Number of pairs is %d" % len(pairs))
print("Number of pairs which non-zero jaccard is %d" % pos_count)
cosine_inx = np.argsort(np.array(list(pcs_cosines)))
x_cosines = np.array(list(pcs_cosines))[cosine_inx] 
y_jaccards = np.array(list(jaccards))[cosine_inx]
# writing points to json
print("Saving pairs and scores...")
with open('testdata/cosines.json', 'w') as fp:
    json.dump(list(cosines), fp)
with open('testdata/pcs_cosines.json', 'w') as fp:
    json.dump(list(pcs_cosines), fp)
with open('testdata/jaccards.json', 'w') as fp:
    json.dump(list(jaccards), fp)
with open('testdata/pairs.json', 'w') as fp:
    json.dump(pairs, fp)
# plotting nearest pc cosines vs jaccards
print("Plotting...")
plt.figure(figsize=(18, 18))
plt.ylabel('nearest pc cosine', fontsize=24)
plt.xlabel('ontology jaccard', fontsize=24)
plt.title('nearest pc cosine vs ontology jaccard of column pairs', fontsize=24)
plt.scatter(jaccards,pcs_cosines)
plt.savefig(args.outputa)
# plotting cosines vs jaccards
plt.figure(figsize=(18, 18))
plt.ylabel('first pc cosine', fontsize=24)
plt.xlabel('ontology jaccard', fontsize=24)
plt.title('first pc cosine vs ontology jaccard of column pairs', fontsize=24)
plt.scatter(jaccards,cosines)
plt.savefig(args.outputb)
print("Done.")
