import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
from networkx.algorithms import bipartite
import argparse, sys
import sqlite3
import os
import json
import time
from itertools import *

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataset", default="/home/fnargesian/WIKI_TABLE/search-index.db")
parser.add_argument("-di", "--domaininfo", default="/home/fnargesian/go/src/github.com/RJMillerLab/table-union/plotting/testdata/domain-db")
parser.add_argument("-oa", "--outputa", default="plots")
args = parser.parse_args(sys.argv[1:])
#
db2 = sqlite3.connect(args.domaininfo)
cursor2 = db2.cursor()
count = 0
ks = [1,2,3,4,5]
cosine_threshold = -1.0
soft_unionable_emb = [{} for i in ks]
hard_unionable_emb = [{} for i in ks]
seen_pairs = []
start_time = time.time()
for table_id1, table_id2 in cursor2.execute("select distinct table_id1, table_id2 from scores;").fetchall():
    for k in range(len(ks)):
        if str(table_id1) not in soft_unionable_emb[k]:
            soft_unionable_emb[k][str(table_id1)] = []
        if str(table_id2) not in soft_unionable_emb[k]:
            soft_unionable_emb[k][str(table_id2)] = []
        if str(table_id1) not in hard_unionable_emb[k]:
            hard_unionable_emb[k][str(table_id1)] = []
        if str(table_id2) not in hard_unionable_emb[k]:
            hard_unionable_emb[k][str(table_id2)] = []
for table_id1, table_id2 in \
    cursor2.execute("select distinct table_id1, table_id2 from scores where best_pc_cosine > ? group by table_id1, table_id2 having count(distinct column_index1) >= ? and count(distinct column_index2) >= ?;", (cosine_threshold, ks[0], ks[0])).fetchall():
    count += 1
    if count % 2 == 0:
        print("Processed %d pairs." % count)
    if (table_id2, table_id1) not in seen_pairs: 
        seen_pairs.append((table_id1, table_id2))
        nodes0 = []
        nodes1 = []
        edges = []
        for column_index1, column_index2 in \
            cursor2.execute("select column_index1, column_index2 from scores where table_id1 = ? and table_id2 = ?;", (table_id1, table_id2)).fetchall():
            nodes0.append(str(table_id1) + "_" + str(column_index1))
            nodes1.append(str(table_id2) + "_" + str(column_index2))
            edges.append((str(table_id1) + "_" + str(column_index1), str(table_id2) + "_" + str(column_index2)))
        M = nx.Graph()
        M.add_nodes_from(nodes0, bipartite=0)
        M.add_nodes_from(nodes1, bipartite=1)
        M.add_edges_from(edges)
        matching = nx.bipartite.maximum_matching(M)
        matched = [k for k,v in matching.items() if k in nodes0]
        for k in ks:
            if len(matched) >= k:
                if str(table_id2) not in soft_unionable_emb[ks.index(k)][str(table_id1)]:
                    soft_unionable_emb[ks.index(k)][str(table_id1)].append(str(table_id2))
                if str(table_id1) not in soft_unionable_emb[ks.index(k)][str(table_id2)]:
                    soft_unionable_emb[ks.index(k)][str(table_id2)].append(str(table_id1))
                hard_cond = True
                for c in list(combinations(nodes0, k)):
                    if not set(c) <= set(matched):
                        hard_cond = False
                        break
                if hard_cond:
                    if str(table_id2) not in hard_unionable_emb[ks.index(k)][str(table_id1)]:
                        hard_unionable_emb[ks.index(k)][str(table_id1)].append(str(table_id2))
                    if str(table_id1) not in hard_unionable_emb[ks.index(k)][str(table_id2)]:
                        hard_unionable_emb[ks.index(k)][str(table_id2)].append(str(table_id1))
print("--- Execution time: %s seconds ---" % (time.time() - start_time))
print("Number of processed tables is %d." % count)
print("Number of k-unionable tables is %d." % len(soft_unionable_emb[0]))
# writing points to json
print("Saving k-unionability info...")
print("Plotting...")
for i in range(len(ks)):
    with open('testdata/' + str(ks[i]) + '_soft_unionable_emb.json', 'w') as fp:
        json.dump(soft_unionable_emb[i], fp)
    with open('testdata/' + str(ks[i]) + '_hard_unionable_emb.json', 'w') as fp:
        json.dump(hard_unionable_emb[i], fp)
print("Done.")
