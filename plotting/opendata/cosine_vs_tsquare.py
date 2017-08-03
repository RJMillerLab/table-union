import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("-dbname", "--dbname", default="/home/fnargesian/TABLE_UNION_OUTPUT/emb-experiments.sqlite")
parser.add_argument("-tablename", "--tablename", default="t2_vs_cosine")
parser.add_argument("-outputa", "--outputa", default="plots/cosine-vs-tsquared.pdf")
#parser.add_argument("-outputa", "--outputa", default="plots/f-vs-tsquared.pdf")
args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.dbname)
cursor = db.cursor()
tablename = args.tablename
# plot the distribution of expansion
fig, ax = plt.subplots(figsize=(10, 10))
ax.set_title('Cosine vs. T Squared in OD', fontsize=18)
ax.set_xlabel('Cosine', fontsize=18)
ax.set_ylabel('Log(T Squared)', fontsize=18)
cosines = []
t2s = []
for j, t in cursor.execute("select cosine, t2 from " + tablename + " where query_cardinality>10 and candidate_cardinality>10;").fetchall():
#and (query_cardinality*1.0/candidate_cardinality*1.0<3 and candidate_cardinality*1.0/query_cardinality*1.0<3);").fetchall():
    if t < 5000:
        cosines.append(j)
        t2s.append(t)
t2s_log = list(np.log(np.asarray(t2s)+1))
print("Number of pairs: %d" % len(t2s))
print("Plotting...")
ax.scatter(cosines, t2s_log, color='y', marker='o', edgecolor='black', alpha=0.5, rasterized=True)
plt.savefig(args.outputa)
#
