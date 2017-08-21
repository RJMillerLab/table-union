import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("-dbname", "--dbname", default="/home/fnargesian/TABLE_UNION_OUTPUT/emb-experiments.sqlite")
parser.add_argument("-tablename", "--tablename", default="t2_vs_cosine")
parser.add_argument("-outputa", "--outputa", default="plots/cosine-vs-u-nl.pdf")
args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.dbname)
cursor = db.cursor()
tablename = args.tablename
# plot the distribution of expansion
cosines = []
us = []
for c, f in cursor.execute("select cosine, f_distribution from " + tablename + " where f_distribution >= 0 and f_distribution <= 1 order by random() limit 7500;").fetchall():
#and (query_cardinality*1.0/candidate_cardinality*1.0<3 and candidate_cardinality*1.0/query_cardinality*1.0<3);").fetchall():
    cosines.append(c)
    us.append(1.0-f)

fig, ax = plt.subplots(figsize=(3, 3))
ax.grid()
ax.set_xlabel('Cosine')
ax.set_ylabel('$U_{nl}$')
ax.set_xlim(0.0, 1.0)
ax.set_ylim(0.0, 1.2)
#ax.set_xticks(np.linspace(0.0, 1.0, ))
ax.scatter(cosines, us, marker='o', alpha=0.2, rasterized=True)
plt.tight_layout()
plt.savefig(args.outputa, bbox_inches='tight', pad_inches=0.02)
