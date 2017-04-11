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

def get_columns(table_dir, table_id, column_id):
   df = pd.read_csv(os.path.join(table_dir, table_id))
   return np.array(df[df.columns[int(column_id)]])


def jaccard_similarity(x,y):
    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    return intersection_cardinality/float(union_cardinality)

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataset", default="/home/ekzhu/WIKI_TABLE/search-index.db")
parser.add_argument("-o", "--output", default="cosine_vs_ontology_jaccard.pdf")
parser.add_argument("-od", "--onttabledir", default="/home/fnargesian/WIKI_TABLE/onttables")
args = parser.parse_args(sys.argv[1:])

cosines = collections.deque([])
jaccards = collections.deque([])
db = sqlite3.connect(args.dataset)
cursor = db.cursor()
dt = np.dtype(float)
dt = dt.newbyteorder('>')
for table_id1, column_index1, bin_vec1, table_id2, column_index2, bin_vec2 in \
        cursor.execute("select t1.table_id as table_id1, t1.column_index as column_id1, t1.vec as bin_vec1, t2.table_id as table_id2, t2.column_index as column_index2, t2.vec as bin_vec2 from (select * from search_index limit 100) t1, (select * from search_index limit 100) t2 where t1.table_id!=t2.table_id;").fetchall():
    vec1 = np.frombuffer(bin_vec1, dtype=dt)
    vec2 = np.frombuffer(bin_vec2, dtype=dt)
    cos = 1.0 - cosine(vec1, vec2)
    cosines.append(cos)
    raw_vec1 = get_columns(args.onttabledir, table_id1, column_index1)
    raw_vec2 = get_columns(args.onttabledir, table_id2, column_index2)
    jac = jaccard_similarity(raw_vec1, raw_vec2) 
    jaccards.append(jac)
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

