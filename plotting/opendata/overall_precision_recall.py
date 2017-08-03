import argparse, sys, os
import matplotlib
import sqlite3
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# since some queries do not have any answer,
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


def get_precision_recall(dbname, tablename, answeredQueries):
    print(tablename)
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
    #return float(correct)/float(returned), float(correct)/float(len(answeredQueries)*25)

parser = argparse.ArgumentParser()
parser.add_argument("-output", "--output", default="plots/precision_recall.pdf")

args = parser.parse_args(sys.argv[1:])
#
benchmarkPath = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4"
measures = ["Set", "Sem-Set", "Sem", "NL", "DasSarma"]
dbs = [os.path.join(benchmarkPath, "jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "ont-jaccard-experiments.sqlite"), os.path.join(benchmarkPath, "emb-experiments.sqlite"), os.path.join(benchmarkPath, "sarma.sqlite")]
tables = ["jaccard_fixedn_backup", "ontology_fixedn_backup", "pure_ontology_fixedn_backup", "t2_fixedn_backup", "topk_sarma"]
#
answeredQueries = get_answered_queries(dbs, tables)
#
ps = []
rs = []
for i in range(len(dbs)):
    p, r = get_precision_recall(dbs[i],tables[i], answeredQueries)
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
    plt.scatter(rs[i], ps[i], color=cs[i], edgecolor = 'black', marker='o', alpha=0.5, s=10**2, label = measures[i])
    if i == 0:
        ax.annotate(measures[i], (rs[i]-dist,ps[i]-dist), ha="right", va="bottom", fontsize=fs)
    if i == 2:
        ax.annotate(measures[i], (rs[i]-dist,ps[i]-dist), ha="right", va="bottom", fontsize=fs)
    if i == 1:
        ax.annotate(measures[i], (rs[i]+3.75*dist,ps[i]-2.25*dist), ha="right", va="bottom", fontsize=fs)
    if i == 3:
        ax.annotate(measures[i], (rs[i]+3*dist,ps[i]-dist), ha="right", va="bottom", fontsize=fs)
    if i == 4:
        ax.annotate(measures[i], (rs[i]+7*dist, ps[i]+dist), ha="right", va="bottom", fontsize=fs)
plt.savefig(args.output, bbox_inches='tight', pad_inches=0.02)
plt.close()
#
print("Done.")
