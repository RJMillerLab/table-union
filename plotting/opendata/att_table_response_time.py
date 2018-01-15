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
        for query_table, response_time in cursor.execute("select query_table, min(duration) as response_time from " + tablename + " where n=" + str(n-1) + " group by query_table order by response_time desc;").fetchall():
            rs.append(response_time)
        rs = np.array(rs)
        all_rs90.append(np.percentile(rs, 90))
        all_rs10.append(np.percentile(rs, 10))
    print("Done res...")
    return all_rs10, all_rs90


parser = argparse.ArgumentParser()
parser.add_argument("-outputc", "--outputc", default="plots/attable_response_time_90p.pdf")
parser.add_argument("-outputb", "--outputb", default="plots/attable_response_time_10p.pdf")
args = parser.parse_args(sys.argv[1:])

ns = np.array([i*10 for i in range(1,7)])
ns = np.array(ns)
all_rs10 = []
all_rs90 = []

dbs = ["scalability.sqlite", "scalability.sqlite", "scalability.sqlite", "scalability.sqlite"]#, "scalability.sqlite"]
tables = ["setnlsem_scores", "nl_scores", "set_scores", "sem_scores"]#, "semset_scores"]
benchmark = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v7"
methods = ["$U_{Ensemble}$", "$U_{NL}$", "$U_{Set}$", "$U_{Sem}$"]#, "$U_{SemSet}$"]

for i in range(len(dbs)):
    rs10,rs90 = get_response_time(os.path.join(benchmark, dbs[i]),tables[i], ns)
    all_rs10.append(rs10)
    all_rs90.append(rs90)

fs = 12
colors = ["royalblue", "g", "r", "c", "m", "y", "k"]
hatches = ["//", "\\\\", ".", "+", "xx"]
width = 1.0
fig, ax = plt.subplots(figsize=(6, 0.618*6))
#ax.set_yscale("log", basey=10)
ax.yaxis.set_major_formatter(ScalarFormatter())
ax.tick_params(axis='both', which='major', labelsize=fs)
for i, ts in enumerate(all_rs90):
    #ts = list(np.log2(np.array(ts)))
    print(ts)
    ax.bar(ns+i*width, ts, width, label=methods[i], color=colors[i], hatch=hatches[i])
ax.set_xticks(ns + width*len(methods)/2.0)
ax.set_xticklabels(ns)
ax.set_xlabel("k", fontsize=fs)
ax.set_ylabel("Response Time (ms)", fontsize=fs)
ax.set_xlim([5,68])
ax.set_ylim([0,170000])
ax.yaxis.grid(linestyle="dotted")
#ax.set_yticks([0,4,8,12,16,20])
ax.set_yticks([0,20000,40000,60000,80000,140000])
ax.legend(loc='best', ncol=len(methods), fontsize=fs, fancybox=True, framealpha=0.1, columnspacing=1.70)
plt.tight_layout()
plt.savefig(args.outputc)
print("Done printing 90 percentile.")
fig, ax = plt.subplots(figsize=(6, 0.618*6))
#ax.set_yscale("log", basey=10)
ax.yaxis.set_major_formatter(ScalarFormatter())
ax.tick_params(axis='both', which='major', labelsize=fs)
for i, ts in enumerate(all_rs10):
    print(methods[i])
    print(ts)
    #ts = list(np.log2(np.array(ts)))
    ax.bar(ns+i*width, ts, width, label=methods[i], color=colors[i], hatch=hatches[i])
ax.set_xticks(ns + width*len(methods)/2.0)
ax.set_xticklabels(ns)
ax.set_xlabel("k", fontsize=fs)
ax.set_yticks([0,200,400,600,800])
ax.set_ylabel("Response Time (ms)", fontsize=fs)
ax.set_xlim([5,68])
ax.set_ylim([0,900])
ax.yaxis.grid(linestyle="dotted")
ax.legend(loc='best', ncol=len(methods), fontsize=fs, fancybox=True, framealpha=0.1, columnspacing=2.25)
plt.tight_layout()
plt.savefig(args.outputb)
print("Done printing 10 percentile.")
print("Done.")

