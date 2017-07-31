import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.style.use("acm-2col.mplstyle")
import sqlite3
import os

def get_table_loss(dbname):
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    all_ms = []
    for n in ns:
        ms = []
        cursor.execute("drop table if exists max_minsim;")
        cursor.execute("create table max_minsim as select query_table, candidate_table from minsim_scores m1 where candidate_table in (select candidate_table from minsim_scores m2 where m1.query_table=m2.query_table order by minsim desc limit " + str(n) + ");")
        cursor.execute("create index max_minsim1 on max_minsim(query_table);")
        cursor.execute("create index max_minsim2 on max_minsim(candidate_table);")
        cursor.execute("drop table if exists max_alignment;")
        cursor.execute("create table max_alignment as select query_table, candidate_table from alignment_scores a1 where candidate_table in (select candidate_table from alignment_scores a2 where a1.query_table=a2.query_table order by alignment_probability desc limit " + str(n) + ");")
        cursor.execute("create index max_alignment1 on max_alignment(query_table);")
        cursor.execute("create index max_alignment2 on max_alignment(candidate_table);")
        cursor.execute("drop table if exists alignment_maxminsim;")
        cursor.execute("create table alignment_maxminsim as select query_table, count(candidate_table) as miss_num from max_alignment a where candidate_table not in (select candidate_table from max_minsim m where m.query_table=a.query_table) group by query_table;")
        for query_table, count in cursor.execute("select query_table, miss_num from alignment_maxminsim;").fetchall():
            ms.append(count)
        all_ms.append(ms)
        print("Done analysis of %d", n)
    return all_ms


parser = argparse.ArgumentParser()
parser.add_argument("-outputa", "--outputa", default="plots/alignment-minsim-probability.pdf")
args = parser.parse_args(sys.argv[1:])

cs = ['g','r','c','m','y','k','orchid']
ns = [3,5,10,15,20,25]
legends = []
benchmarkPath = "/home/fnargesian/TABLE_UNION_OUTPUT"
#measures = ["syntactic domain", "semantic and syntactic domain", "natural language domain", "semantic domain"]
#dbs = [os.path.join(benchmarkPath, "jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "emb-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite")]
#dbs = [os.path.join(benchmarkPath, "emb-experiments.sqlite")]
dbs = [os.path.join(benchmarkPath, "ontology-experiments.sqlite")]
#tables = ["jaccard_fixedn", "ontology_fixedn", "t2_fixedn", "pure_ontology_fixedn"]
markers = ["x", "+", "*", "o"]
plt.figure(figsize=(5, 5))
plt.grid(linestyle='dashed')
plt.ylabel('Number of Missed Candidates', fontsize=12)
plt.xlabel('Query Tables', fontsize=12)
#plt.title('Number of Missed Candidates due to using Minsim instead of Max Alignment Probability')
plt.xlim(0.0,550)
plt.ylim(0,25)
dbname = dbs[0]
all_ms = get_table_loss(dbname)
ts = [i for i in range(500)]
i = 0
for ms in all_ms:
    for j in range(len(ms), 500):
        ms.append(0)
    ms.sort()
    plt.plot(ts, ms, color=cs[i], linewidth=3)
    i += 1
plt.tight_layout()
plt.savefig(args.outputa)
print("Done.")


