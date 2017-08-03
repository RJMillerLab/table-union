import argparse, sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
#plt.style.use("acm-2col.mplstyle")
import sqlite3
import os
from matplotlib.ticker import ScalarFormatter

def get_response_time(dbname, tablename, ns):
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    all_rs10 = []
    all_rs90 = []
    for n in ns:
        rs = []
        for query_table, response_time in cursor.execute("select query_table, min(duration) as response_time from " + tablename + " where n=" + str(n) + " group by query_table order by response_time desc;").fetchall():
            rs.append(response_time)
        rs = np.array(rs)
        all_rs90.append(np.percentile(rs, 90))
        all_rs10.append(np.percentile(rs, 10))
    print("Done res...")
    return all_rs10, all_rs90


parser = argparse.ArgumentParser()
parser.add_argument("-outputc", "--outputc", default="plots/small_response_time.pdf")
args = parser.parse_args(sys.argv[1:])

ns = np.array([5, 10, 15, 20, 25])
all_rs10 = []
all_rs90 = []
benchmarkPath = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v3"
measures = ["Set", "Sem", "Set-Sem", "NL"]
dbs = [os.path.join(benchmarkPath, "jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "emb-experiments.sqlite")]
tables = ["jaccard_fixedn", "pure_ontology_fixedn", "ontology_fixedn", "t2_fixedn_v1"]
for i in range(len(dbs)):
    rs10,rs90 = get_response_time(dbs[i],tables[i], ns)
    all_rs10.append(rs10)
    all_rs90.append(rs90)

# ERIC: we just need to 90 pct...
markersize = 3
linestyles = ["-+", "-x", "-*", "-o"]
cs = ['g','c', 'y', 'b']
hatches = ['//', '\\', '/', 'o']
width = 1.0
fig, ax = plt.subplots(figsize=(6, 0.618*6))
ax.set_yscale("log", basey=10)
ax.yaxis.set_major_formatter(ScalarFormatter())
ax.grid()
for i, ts in enumerate(all_rs90):
    ax.bar(ns+i*width, ts, width, label=measures[i], color=cs[i], hatch=hatches[i])
ax.set_xticks(ns + width*len(measures)/2.0)
ax.set_xticklabels(ns)
ax.set_xlabel("k")
ax.set_ylabel("Response Time (ms)")
ax.legend(loc='best', ncol=len(measures), fontsize=12, fancybox=True, framealpha=0.1)
plt.tight_layout()
plt.savefig(args.outputc)
print("Done.")

