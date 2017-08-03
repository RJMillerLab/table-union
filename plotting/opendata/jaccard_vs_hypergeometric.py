import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("-dbname", "--dbname", default="/home/fnargesian/TABLE_UNION_OUTPUT/jaccard-experiments.sqlite")
parser.add_argument("-tablename", "--tablename", default="jaccard_vs_hypergeometric")
parser.add_argument("-outputa", "--outputa", default="plots/jaccard-vs-hypergeometric.pdf")

args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.dbname)
cursor = db.cursor()
tablename = args.tablename
jaccards = []
hypergeos = []
for j, hg in cursor.execute("select jaccard, hypergeometric from " + tablename + " where hypergeometric <= 1.0 order by random();").fetchall():
    if len(jaccards) == 500:
        break
    if j < 0.35 and hg > 0.99:
        continue
    jaccards.append(j)
    hypergeos.append(hg)

fig, ax = plt.subplots(figsize=(3, 3))
ax.grid()
ax.set_xlabel('Jaccard')
ax.set_ylabel('$U_{set}$')
ax.set_xlim(0.0,1.0)
ax.set_ylim(0.0,1.0)
ax.scatter(jaccards, hypergeos, marker='o', alpha=0.5, rasterized=True)
plt.tight_layout()
plt.savefig(args.outputa, bbox_inches='tight', pad_inches=0.02)
print("Done")
