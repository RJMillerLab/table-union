import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
#plt.style.use("acm-2col.mplstyle")
import sqlite3
import os

def get_table_loss(dbname):
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    all_ms = []
    for n in ns:
        ms = []
        #cursor.execute("drop table if exists max_minsim_" + str(n) + ";")
        #cursor.execute("create table max_minsim_" + str(n) + " as select query_table, candidate_table from minsim_scores m1 where candidate_table in (select candidate_table from minsim_scores m2 where m1.query_table=m2.query_table order by minsim desc limit " + str(n) + ");")
        #cursor.execute("create index max_minsim1_" + str(n) + " on max_minsim_" + str(n) + "(query_table);")
        #cursor.execute("create index max_minsim2_" + str(n) + " on max_minsim_" + str(n) + "(candidate_table);")
        #cursor.execute("drop table if exists max_alignment_" + str(n) + ";")
        #cursor.execute("create table max_alignment_" + str(n) + " as select query_table, candidate_table from alignment_scores a1 where candidate_table in (select candidate_table from alignment_scores a2 where a1.query_table=a2.query_table order by alignment_probability desc limit " + str(n) + ");")
        #cursor.execute("create index max_alignment1_"  + str(n) + " on max_alignment_" + str(n) + "(query_table);")
        #cursor.execute("create index max_alignment2_" + str(n) + " on max_alignment_" + str(n) + "(candidate_table);")
        #cursor.execute("drop table if exists alignment_maxminsim_" + str(n) + ";")
        #cursor.execute("create table alignment_maxminsim_" + str(n) + " as select query_table, count(candidate_table) as miss_num from max_alignment_" + str(n) + " a where candidate_table not in (select candidate_table from max_minsim_" + str(n) + " m where m.query_table=a.query_table) group by query_table;")
        for query_table, count in cursor.execute("select query_table, miss_num from alignment_maxminsim_" + str(n) + ";").fetchall():
            ms.append(count)
        for j in range(len(ms), 500):
            ms.append(0)
        all_ms.append(ms)
        print("Done analysis of %d", n)
    return all_ms


parser = argparse.ArgumentParser()
parser.add_argument("-outputa", "--outputa", default="plots/alignment-minsim-probability.pdf")
args = parser.parse_args(sys.argv[1:])

ns = [3,5,10,15,20,25]
benchmarkPath = "/home/fnargesian/TABLE_UNION_OUTPUT"
dbs = [os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite")]

fs = 7
markersize = 1.5
lw = 1
cs = ['g','r','b','y','k','orchid']
linestyles = ["-+", "-x", "-*", "-o", "-^", "-."]

fig, ax = plt.subplots(1, 1, figsize=(4,2.2))
ax.tick_params(axis='both', which='major', labelsize=fs)
ax.grid(linestyle='--')
ax.set_xlabel('Recall', fontsize=fs)
ax.set_ylabel('Precision', fontsize=fs)
ax.set_xlim([0,510])
ax.set_ylim([0,25])
markers = ["x", "+", "*", "o", "^", "."]
ax.set_ylabel('Number of Missed Candidates', fontsize=fs)
ax.set_xlabel('Query Tables', fontsize=fs)
ax.set_axisbelow(True)

dbname = dbs[0]
all_ms = get_table_loss(dbname)
ts = [i for i in range(500)]
i = 0
for ms in all_ms:
    ms.sort()
    ax.plot(ts, ms, linestyles[i], label="k = " + str(ns[i]),  markersize=markersize, color = cs[i],    linewidth=lw)
    i += 1
lgd = plt.legend(ncol=1, loc="upper left", bbox_transform=plt.gcf().transFigure, fontsize=fs)
plt.savefig(args.outputa, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=0.02)
plt.close()
print("Done.")


