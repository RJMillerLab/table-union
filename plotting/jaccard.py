import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
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
parser.add_argument("-d", "--dataset", default="/home/ekzhu/WIKI_TABLE/search-index.db")
parser.add_argument("-o", "--output", default="jaccard.pdf")
parser.add_argument("-otd", "--onttabledir", default="/home/fnargesian/WIKI_TABLE/onttables")
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
# generating domain pairs 
count = 0
equal_domains = 0
db2 = sqlite3.connect("testdata/domain-db")
cursor2 = db2.cursor()
#
print("Computing jaccard...")
for table_id1, column_index1, table_id2, column_index2 in \
    cursor2.execute("select distinct t1.table_id as table_id1, t1.column_index as column_id1, t2.table_id as table_id2, t2.column_index as column_index2 from (select distinct table_id, column_index from domains where ontology_domain_size>25) t1, (select distinct table_id, column_index from domains where ontology_domain_size>25) t2 where t1.table_id!=t2.table_id;").fetchall():
     #cursor.execute("select distinct t1.table_id as table_id1, t1.column_index as column_id1, t2.table_id as table_id2, t2.column_index as column_index2 from (select distinct table_id, column_index from search_index order by random() limit 200) t1, (select distinct table_id, column_index from search_index order by random() limit 200) t2 where t1.table_id!=t2.table_id;").fetchall():
    if (table_id1, column_index1, table_id2, column_index2) not in seen_pairs and \
            (table_id2, column_index2, table_id1, column_index1) not in seen_pairs:
        count += 1
        if count % 300 == 0:
            print("Finished processing %d pairs." % count)
        dom1 = get_domain(args.onttabledir, table_id1, column_index1)
        dom2 = get_domain(args.onttabledir, table_id2, column_index2)
        #if len(dom1) < 5 or len(dom2) < 5:
        #    continue
        if is_equal(dom1, dom2):
            equal_domains += 1
            continue
        jac = jaccard_similarity(dom1, dom2)
        if jac == None:
            continue
        if jac != 0.0:
            pairs.append((table_id1, column_index1, table_id2, column_index2))
            jaccards.append(jac)
        seen_pairs.append((table_id1, column_index1, table_id2, column_index2))

print("Number of processed pairs is %d" % count)
print("Number of pairs which non-zero jaccard is %d" % len(pairs))
# writing points to json
print("Saving pairs and jaccard scores...")
with open('testdata/jaccards.json', 'w') as fp:
    json.dump(list(jaccards), fp)
with open('testdata/pairs.json', 'w') as fp:
    json.dump(pairs, fp)
# plotting jaccards
print("Plotting...")
plt.figure(figsize=(18, 18))
xs = [i for i in range(len(pairs))]
plt.ylabel('ontology jaccard score', fontsize=24)
plt.xlabel('domain pairs', fontsize=24)
plt.title('ontology jaccard of domain pairs', fontsize=24)
plt.scatter(xs, jaccards)
plt.savefig(args.output)
print("Done.")
