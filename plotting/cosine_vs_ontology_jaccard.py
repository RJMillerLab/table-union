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
        jac = -1.0
    return jac


def mk_entity_index(domain, table_id, column_index):
    for val in list(domain):
        if val in index:
            if (table_id, column_index) not in index[val]:
                index[val].append((table_id, column_index))
        else:
            index[val] = [(table_id, column_index)]



parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataset", default="/home/fnargesian/WIKI_TABLE/search-index.db")
parser.add_argument("-o", "--output", default="cosine_vs_ontology_jaccard.pdf")
parser.add_argument("-od", "--onttabledir", default="/home/fnargesian/WIKI_TABLE/onttables")
args = parser.parse_args(sys.argv[1:])

index = {}
neg_samples = []
seen_pairs = []
cosines = collections.deque([])
jaccards = collections.deque([])
db = sqlite3.connect(args.dataset)
cursor = db.cursor()
dt = np.dtype(float)
dt = dt.newbyteorder('>')
for table_id1, column_index1, bin_vec1, table_id2, column_index2, bin_vec2 in \
        cursor.execute("select t1.table_id as table_id1, t1.column_index as column_id1, t1.vec as bin_vec1, t2.table_id as table_id2, t2.column_index as column_index2, t2.vec as bin_vec2 from (select * from search_index order by random() limit 1200) t1, (select * from search_index order by random() limit 1200) t2 where t1.table_id!=t2.table_id;").fetchall():
    if (table_id1, column_index1, table_id2, column_index2) not in seen_pairs and \
            (table_id2, column_index2, table_id1, column_index1) not in seen_pairs:
        raw_vec1 = get_domain(args.onttabledir, table_id1, column_index1)
        raw_vec2 = get_domain(args.onttabledir, table_id2, column_index2)
        jac = jaccard_similarity(raw_vec1, raw_vec2)
        if jac > 0:
            mk_entity_index(raw_vec1, table_id1, column_index1)
            mk_entity_index(raw_vec2, table_id2, column_index2)
        elif jac != -1:
            neg_samples.append((table_id1, column_index1, table_id2, column_index2))

print("size of inverted index is : %d" % len(index))

for e, ds in index.items():
    for d1 in ds:
        for d2 in ds:
            if not (d1[0] == d2[0] and d1[1] == d2[1]):
                raw_vec1 = get_domain(args.onttabledir, d1[0], d1[1])
                raw_vec2 = get_domain(args.onttabledir, d2[0], d2[1])
                jac = jaccard_similarity(raw_vec1, raw_vec2) 
                print(jac)
                if jac != -1.0:
                    bin_vec1 = cursor.execute("select vec from search_index where table_id=? and column_index=?", (d1[0], d1[1])).fetchone()[0]
                    bin_vec2 = cursor.execute("select vec from search_index where table_id=? and column_index=?", (d2[0], d2[1])).fetchone()[0]
                    vec1 = np.frombuffer(bin_vec1, dtype=dt)
                    vec2 = np.frombuffer(bin_vec2, dtype=dt)
                    cos = 1.0 - cosine(vec1, vec2)
                    cosines.append(cos)
                    jaccards.append(jac)
                    seen_pairs.append((table_id1, column_index1, table_id2, column_index2))
                    # print similar columns

print("len of pos samples is %d\n" % len(cosines))
pos_samples = cosines

for i in range(len(pos_samples)):
    ns = neg_samples[i]
    bin_vec1 = cursor.execute("select vec from search_index where table_id=? and column_index=?", (ns[0], ns[1])).fetchone()[0]
    bin_vec2 = cursor.execute("select vec from search_index where table_id=? and column_index=?", (ns[2], ns[3])).fetchone()[0]
    vec1 = np.frombuffer(bin_vec1, dtype=dt)
    vec2 = np.frombuffer(bin_vec2, dtype=dt)
    cos = 1.0 - cosine(vec1, vec2)
    cosines.append(cos)
    jaccards.append(0.0)

print("len of samples is %d\n" % len(cosines))
cosine_inx = np.argsort(np.array(list(cosines)))
x_cosines = np.array(list(cosines))[cosine_inx] 
y_jaccards = np.array(list(jaccards))[cosine_inx]

# plotting cosines vs jaccards
plt.figure(figsize=(18, 18))
plt.xlabel('pca cosine', fontsize=24)
plt.ylabel('ontology jaccard', fontsize=24)
plt.title('pca cosine vs ontology jaccard of column pairs', fontsize=24)
plt.scatter(cosines, jaccards)
plt.savefig(args.output)

