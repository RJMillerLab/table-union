import argparse, sys
import matplotlib
matplotlib.use("Agg")
from matplotlib.ticker import ScalarFormatter
import matplotlib.pyplot as plt
import sqlite3
import os
import numpy as np


def get_response_time(dbname, tablename):
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    rs = []
    for query_table, response_time in cursor.execute("select query_table, min(duration) as response_time  from " + tablename + " where n=9 group by query_table order by response_time desc;").fetchall():
        rs.append(response_time)
    rs = np.array(rs)
    rs90 = np.percentile(rs, 90)
    rs10 = np.percentile(rs, 10)
    print("Done res...")
    return rs10, rs90


parser = argparse.ArgumentParser()
parser.add_argument("-outputa", "--outputa", default="plots/attable_scalability_response_time.pdf")
args = parser.parse_args(sys.argv[1:])

all_rs10 = []
all_rs90 = []
benchmarkPath = "/home/fnargesian/TABLE_UNION_OUTPUT"
db = os.path.join(benchmarkPath, "scalability.sqlite")
tables = ["scores200", "scores400", "scores600", "scores800", "scores1m"]
markers = ["x", "+", "*", "o", "^"]


for i in range(len(tables)):
    if i==6:
        all_rs90.append(0)
        all_rs10.append(0)
        continue
    rs10, rs90 = get_response_time(db, tables[i])
    all_rs90.append(rs90)
    all_rs10.append(rs10)

all_rs10.sort()
all_rs90.sort()
all_rs = []
all_rs.append(all_rs90)
all_rs.append(all_rs10)
#
index_size = [1, 2, 3, 4, 5]
fs = 12
linestyles = ["-+", "-x", "--", "-"]
cs = ['royalblue','c', 'y', 'b']
hatches = ['//', '\\', '/', 'o']
width = 2.0#1.0
fig, ax = plt.subplots(figsize=(6, 0.618*6))
#ax.set_yscale("log", basey=2)
ax.yaxis.set_major_formatter(ScalarFormatter())
index_labels = ["200K", "400K", "600K", "800K", "1M"]
ns = np.array([5, 10, 15, 20, 25])
labels = ["90-Percentile", "10-Percentile"]
for i, ts in enumerate(all_rs):
    if i == 1:
        continue
    ax.bar(ns+i*width, ts, width, label=labels[i], color=cs[i], hatch=hatches[i])
ax.set_axisbelow(True)
#
ax.yaxis.grid(linestyle="dotted")
#ax.set_xticks(ns + width*len(labels)/2.0)
ax.set_xticks(ns)
ax.set_xticklabels(index_labels)
for tick in ax.get_xticklabels():
    tick.set_rotation(45)
ax.set_xlabel('Corpus Size', fontsize=fs)
ax.set_ylabel('Response Time (ms)', fontsize=fs)
ax.set_ylim(0,6300)
#ax.legend(loc='best', ncol=len(labels), fontsize=10, fancybox=True, framealpha=0.1)
plt.tight_layout()
plt.savefig(args.outputa)
plt.close()
print("Done.")

