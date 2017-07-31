import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3
import os

def get_measure_accuracy(dbname, tablename):
    print(tablename)
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    ps = []
    rs = []
    queryNum = 0
    for n in ns:
    #for query_table in cursor.execute("select distinct query_table from " + tablename + ";").fetchall():
        queryNum += 1
        correct = {}
        returned = {}
        for query_table, count in cursor.execute("select query_table, count(candidate_table) as correct from (select distinct query_table, candidate_table from " + tablename + " where n<=" + str(n) + ") natural join groundtruth group by query_table;").fetchall():
        #for count in cursor.execute("select count(candidate_table) as correct from  (select distinct query_table, candidate_table from " + tablename + " where query_table='" + query_table[0] + "' order by alignment_probability desc) natural join groundtruth;").fetchall():
            queryNum += 1
            correct[query_table] = count
        for query_table, total in cursor.execute("select query_table, count(*) as total from (select distinct query_table, candidate_table from " + tablename + " where n<=" + str(n) + ") group by query_table;").fetchall():
            returned[query_table] = total
        sumPrecision = 0.0
        sumRecall = 0.0
        for q, s in returned.items():
            if q in correct:
                sumPrecision += (float(correct[q])/float(s))
                sumRecall += (float(correct[q])/float(max(ns)))
        ps.append(sumPrecision/float(len(returned)))
        rs.append(sumRecall/float(len(returned)))
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
measures = ["Set", "Sem-Set", "Natural Language", "Sem"]
dbs = [os.path.join(benchmarkPath, "jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "emb-experiments.sqlite"), os.path.join(benchmarkPath,"ont-jaccard-experiments.sqlite")]
tables = ["jaccard_fixedn", "ontology_fixedn_backup", "t2_fixedn_backup", "pure_ontology_fixedn_backup"]
markers = ["x", "+", "*", "o"]
for i in range(len(dbs)):
    ps, rs = get_measure_accuracy(dbs[i],tables[i])
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
ax.set_ylim([0,1])
ax.set_axisbelow(True)

for i in range(len(dbs)):
    ax.plot(all_rs[i], all_ps[i], linestyles[i], label=measures[i],  markersize=markersize, color = cs[i], linewidth=lw)
lgd = plt.legend(ncol=1, loc="lower right", bbox_transform=plt.gcf().transFigure, fontsize=fs)
#lgd = plt.legend(ncol=1, mode="expand", loc="upper left", bbox_to_anchor=(0.90, 0.87, 0.28, -0.05),           bbox_transform=plt.gcf().transFigure, fontsize=fs)
plt.savefig(args.outputa, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=0.02)
plt.close()
print("Done.")


