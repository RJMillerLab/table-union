import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial.distance import cosine 
import sqlite3
import collections

def quantiles(cosines):
    qs = np.arange(10, 100, 10)
    ps = np.percentile(cosines, qs) 
    for q, p in zip(qs, ps):
        print("%d Percentile = %.4f" % (q, p))


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--labeled-dataset", default="/home/ekzhu/WWT/labeled-column-pairs.db")
parser.add_argument("-o", "--output", default="cosine_vs_annotation_link.pdf")
args = parser.parse_args(sys.argv[1:])

neg_cosines = collections.deque([])
pos_cosines = collections.deque([])
db = sqlite3.connect(args.labeled_dataset)
cursor = db.cursor()
dt = np.dtype(float)
dt = dt.newbyteorder('>')
for table_id1, table_id2, column_index1, column_index2, label in \
        cursor.execute("select table_id1, table_id2, column_index1, column_index2, label from column_pair;").fetchall():
    bin_vec1 = cursor.execute("select vec from domain_vec where table_id=? and column_index=?", (table_id1, column_index1)).fetchone()[0] 
    bin_vec2 = cursor.execute("select vec from domain_vec where table_id=? and column_index=?", (table_id2, column_index2)).fetchone()[0]
    vec1 = np.frombuffer(bin_vec1, dtype=dt)
    vec2 = np.frombuffer(bin_vec2, dtype=dt)
    cos = cosine(vec1, vec2)
    if label == 0:
        neg_cosines.append(cos)
    else:
        pos_cosines.append(cos)
print("Finish loading dataset")
neg_cosines = np.sort(np.array(list(neg_cosines)))
pos_cosines = np.sort(np.array(list(pos_cosines)))

# Computing quantiles
print("Negative pairs cosine quantiles")
quantiles(neg_cosines)
print("Positive pairs cosine quantiles")
quantiles(pos_cosines)

