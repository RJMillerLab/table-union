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
# plot the distribution of expansion
fig, ax = plt.subplots(figsize=(10, 10))
ax.set_title('Jaccard vs. the Cumulative Distribution of Hypergeometric in OD', fontsize=18)
ax.set_xlabel('Jaccard', fontsize=18)
ax.set_ylabel('F(k)', fontsize=18)
jaccards = []
hypergeos = []
for j, hg in cursor.execute("select jaccard, hypergeometric from " + tablename + ";").fetchall():
    if j < 0.35 and hg > 0.99:
        continue
    jaccards.append(j)
    hypergeos.append(hg)
print("Number of jaccard pairs: %d" % len(hypergeos))
print("Plotting...")
ax.scatter(jaccards, hypergeos, color='y', marker='o', edgecolor='black', alpha=0.5, rasterized=True)
plt.savefig(args.outputa)
print("Done")
#
