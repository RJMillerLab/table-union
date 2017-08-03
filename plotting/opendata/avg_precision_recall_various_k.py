import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3
import os

# first we count the number of queries that are answered by some
# measure.
def get_answered_queries(dbnames, tablenames):
    queries = {}
    for i in range(len(dbnames)):
        db = dbnames[i]
        table = tablenames[i]
        db = sqlite3.connect(db)
        cursor = db.cursor()
        for query_name in cursor.execute("select distinct query_table from " + table + ";").fetchall():
            queries[query_name] = 1
    return queries

def get_measure_accuracy(dbname, tablename, queries):
    print(tablename)
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    ps = []
    rs = []
    queryNum = 0
    for n in ns:
        queryNum += 1
        correct = {}
        returned = {}
        for query_table, count in cursor.execute("select query_table, count(candidate_table) as correct from (select distinct query_table, candidate_table from " + tablename + " where n<=" + str(n) + ") natural join groundtruth group by query_table;").fetchall():
            queryNum += 1
            correct[query_table] = count
        for query_table, total in cursor.execute("select query_table, count(*) as total from (select distinct query_table, candidate_table from " + tablename + " where n<=" + str(n) + ") group by query_table;").fetchall():
            returned[query_table] = total
        sumPrecision = 0.0
        sumRecall = 0.0
        for q, s in returned.items():
            if q in correct:
                sumPrecision += (float(correct[q])/float(s))
                sumRecall += (float(correct[q])/float(len(ns)))
        ps.append(sumPrecision/float(len(returned)))
        rs.append(sumRecall/float(len(queries)))
    print("Done precision...")
    return ps, rs


parser = argparse.ArgumentParser()
parser.add_argument("-outputa", "--outputa", default="plots/k-avg-precision-recall.pdf")
args = parser.parse_args(sys.argv[1:])

ns = [i for i in range(1,26)]
legends = []
all_ps = []
all_rs = []
benchmarkPath = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4"
measures = ["Set", "Sem-Set", "NL", "Sem"]
dbs = [os.path.join(benchmarkPath, "jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "emb-experiments.sqlite"), os.path.join(benchmarkPath,"ont-jaccard-experiments.sqlite")]
tables = ["jaccard_fixedn", "ontology_fixedn", "t2_fixedn_backup", "pure_ontology_fixedn"]
markers = ["x", "+", "*", "o"]

answeredQueries = get_answered_queries(dbs, tables)

for i in range(len(dbs)):
    ps, rs = get_measure_accuracy(dbs[i],tables[i], answeredQueries)
    all_ps.append(ps)
    all_rs.append(rs)

fs = 7
dist = 0.05
markersize = 1.5
lw = 1
linestyles = ["-+", "-x", "-*", "-o"]
cs = ['g','r','c','m','y','k','orchid']
fig, ax = plt.subplots(1, 1, figsize=(2.2, 2.2))
ax.tick_params(axis='both', which='major', labelsize=fs)
ax.grid(linestyle='--')
ax.set_xlabel('Recall', fontsize=fs)
ax.set_ylabel('Precision', fontsize=fs)
ax.set_xlim([0,1])
ax.set_ylim([0.5,1])
ax.set_axisbelow(True)

for i in range(len(dbs)):
    ax.plot(all_rs[i], all_ps[i], linestyles[i], label=measures[i],  markersize=markersize, color = cs[i], linewidth=lw)
lgd = plt.legend(ncol=1, loc="lower right", bbox_transform=plt.gcf().transFigure, fontsize=fs)
plt.savefig(args.outputa, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=0.02)
plt.close()
print("Done.")

