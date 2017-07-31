import argparse, sys, os
import matplotlib
import sqlite3
matplotlib.use("Agg")
import matplotlib.pyplot as plt

def get_precision_recall(dbname, tablename):
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    returned = 0
    correct = 0
    allcorrect = 0
    for a in cursor.execute("select count(*) from groundtruth;"):
        allcorrect = a[0]
    for r in cursor.execute("select count(*) from (select distinct query_table, candidate_table from " + tablename + ");"):
        returned = r[0]
    for c in cursor.execute("select count(*) from (select distinct query_table, candidate_table from " +tablename + " natural join groundtruth);"):
        correct = c[0]
    print("allcorrect: %d returned: %d correct: %d" %(allcorrect, returned, correct))
    return float(correct)/float(returned), float(correct)/float(allcorrect)


parser = argparse.ArgumentParser()
parser.add_argument("-output", "--output", default="plots/precision_recall.pdf")

args = parser.parse_args(sys.argv[1:])
#
benchmarkPath = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4"
measures = ["Set", "Sem", "Sem-Set", "NL", "DasSarma"]
dbs = [os.path.join(benchmarkPath, "jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "emb-experiments.sqlite"), os.path.join(benchmarkPath, "sarma.sqlite")]
tables = ["jaccard_fixedn", "pure_ontology_fixedn_backup", "ontology_fixedn_backup", "t2_fixedn_backup", "topk_sarma"]
#
ps = []
rs = []
for i in range(len(dbs)):
    p, r = get_precision_recall(dbs[i],tables[i])
    ps.append(p)
    rs.append(r)
#
fs = 7
dist = 0.05
cs = ['g','r','b','y','k','orchid']
fig, ax = plt.subplots(1, 1, figsize=(2.2, 2.2))
ax.tick_params(axis='both', which='major', labelsize=fs)
ax.grid(linestyle='--')
ax.set_xlabel('Recall', fontsize=fs)
ax.set_ylabel('Precision', fontsize=fs)
ax.set_xlim([0,1])
ax.set_ylim([0,1])
ax.set_axisbelow(True)
for i in range(len(dbs)):
    plt.scatter(rs[i], ps[i], color=cs[i], edgecolor = 'black', marker='o', alpha=0.5, rasterized=True, s=10**2, label = measures[i])
    if i == 0:
        ax.annotate(measures[i], (rs[i]-dist,ps[i]), ha="right", va="bottom", fontsize=fs)
    if i == 2:
        ax.annotate(measures[i], (rs[i],ps[i]+dist), ha="right", va="bottom", fontsize=fs)
    if i == 1:
        ax.annotate(measures[i], (rs[i],ps[i]-2*dist), ha="right", va="bottom", fontsize=fs)
    if i == 3:
        ax.annotate(measures[i], (rs[i]+3*dist,ps[i]), ha="right", va="bottom", fontsize=fs)
    if i == 4:
        ax.annotate(measures[i], (rs[i]+dist, ps[i]), ha="right", va="bottom", fontsize=fs)
plt.savefig(args.output, bbox_inches='tight', pad_inches=0.02)
plt.close()
#
print("Done.")
