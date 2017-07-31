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

ns = [i for i in range(1,26)]
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
#
markersize = 3
linestyles = ["-+", "-x", "-*", "-o"]
cs = ['g','r','c','m','y','k','orchid']
fs = 7
#
fig, ax = plt.subplots(1, 1, figsize=(4, 1.2))
ax.tick_params(axis='both', which='major', labelsize=fs)
ax.grid(linestyle='--')
#
ax.plot(ns, list(np.log2(np.array(all_rs10[0]))), linestyles[0], label=measures[0], markersize=markersize, color = cs[0])
ax.plot(ns, list(np.log2(np.array(all_rs90[0]))), linestyles[1], label=measures[1], markersize=markersize,color = cs[1])
ax.set_ylabel('Response Time (log2(ms))', fontsize=fs)
ax.set_xlabel('K', fontsize=fs)
ax.set_xlim(0,25)
ax.set_title('Natural Domain Unionability', fontsize=fs)
#
lgd = plt.legend(ncol=1, loc="center right", bbox_transform=plt.gcf().transFigure, fontsize=fs)
plt.savefig(args.output, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=0.02)
plt.close()
#
