import argparse, sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
#plt.style.use("acm-2col.mplstyle")
import sqlite3
import os

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
parser.add_argument("-output", "--output", default="plots/uk_ca_response_time.pdf")
args = parser.parse_args(sys.argv[1:])

ns = np.array([5, 10, 15, 20, 25])
all_rs10 = []
all_rs90 = []
benchmarkPath = "/home/fnargesian/TABLE_UNION_OUTPUT"
measures = ["10-Percentile", "90-Percentile"]
dbs = [os.path.join(benchmarkPath,"emb-experiments.sqlite")]
tables = ["uk_ca_t2_fixedn"]
for i in range(len(dbs)):
    rs10,rs90 = get_response_time(dbs[i],tables[i], ns)
    all_rs10.append(rs10)
    all_rs90.append(rs90)

markersize = 3
linestyles = ["-+", "-x", "-*", "-o"]
cs = ['g','c', 'y', 'b']
width = 1.65
fig, ax = plt.subplots(figsize=(6, 0.618*6))
ax.grid()
ax.set_ylim(100, 800)
ax.bar(ns-width, all_rs10[0], width, color='g', label="10-Pct", hatch="//")
ax.bar(ns, all_rs90[0], width, color='c', label='90-Pct', hatch='\\')
ax.set_xticks(ns)
ax.set_xticklabels(ns)
ax.set_xlabel("k")
ax.set_ylabel("Response Time (ms)")
ax.legend(loc='best', ncol=2, fontsize=12, fancybox=True, framealpha=0.1)
plt.tight_layout()
plt.savefig(args.output)
print("Done.")

