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
        #for query_name in cursor.execute("select distinct query_table from " + table + " where query_table!=candidate_table;").fetchall():
        for query_name in cursor.execute("select distinct query_table from " + table + ";").fetchall():
            queries[query_name] = 1
    return queries


def get_precision_recall(dbname, tablename, querynum, k):
    print(tablename)
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    #returned = 0
    #correct = 0
    precision = 0.0
    recall = 0.0
    cursor.execute("create index if not exists " + tablename +"_inx on " + tablename + "(query_table, candidate_table);")
    cursor.execute("Drop table if exists scores_c;")
    cursor.execute("create table scores_c as select p.query_table as query_table, p.candidate_table as candidate_table, count(distinct p.query_col_name) as c from " + tablename + " p, (select query_table, candidate_table, query_col_name, candidate_col_name from all_groundtruth) g where p.query_table=g.query_table and p.candidate_table=g.candidate_table and lower(p.query_col_name)=lower(g.query_col_name) and lower(p.candidate_col_name)=lower(g.candidate_col_name) group by p.query_table, p.candidate_table;")
    cursor.execute("create index scinx1 on scores_c(query_table);")
    cursor.execute("create index scinx2 on scores_c(candidate_table);")
    # number of correct alignments
    for a in cursor.execute("select avg(query_precision) as precision from (Select a.query_table as query_table, 1.0*count(a.candidate_table)/min((select min(65,count(distinct candidate_table)) from " + tablename + " where query_table=a.query_table), (select unionable_count from recall_groundtruth where query_table=a.query_table)) as query_precision from (select distinct s.query_table as query_table, s.candidate_table as candidate_table from scores_c s,groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c=g.c) a group by a.query_table);").fetchall():
        #"select sum(query_precision) from (Select a.query_table as query_table, 1.0*count(a.candidate_table)/(1.0*(select count(distinct candidate_table) from " + tablename + " t where t.query_table=a.query_table)) as query_precision from (select distinct s.query_table as query_table, s.candidate_table as candidate_table from scores_c s, groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c=g.c) a group by a.query_table);").fetchall():
        precision = a[0]
    for a in cursor.execute("select avg(query_recall) as recall from (Select a.query_table as query_table, 1.0*count(a.candidate_table)/min(65, (select unionable_count from recall_groundtruth where query_table=a.query_table)) as query_recall from (select distinct s.query_table as query_table, s.candidate_table as candidate_table from scores_c s, groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c=g.c) a group by a.query_table);").fetchall():
        recall = a[0]
    # number of returbed results
    #for query_num in cursor.execute("select count(distinct query_table) from " + tablename + ";").fetchall():
    #    returned_querynum = query_num[0]

    db.close()
    return precision, recall
    #return float(correct)/float(returned_querynum), float(correct)/float(querynum)

parser = argparse.ArgumentParser()
parser.add_argument("-output", "--output", default="plots/att_table_accuracy_all.pdf")

args = parser.parse_args(sys.argv[1:])
#
dbs = ["octopus_1500_300.sqlite", "octopus_1500_300.sqlite", "tablesearch_results.sqlite", "tablestitching.sqlite"]#, "tablesearch_results.sqlite", "tablesearch_results.sqlite", "tablesearch_results.sqlite", "tablesearch_results.sqlite"]
tables = ["column_text_alignment_unionable", "size_alignment_unionable", "benchmark1500300", "stitching_unionable_tables"]#, "nl_scores", "set_scores", "sem_scores", "semset_scores"]
benchmark = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v6"
methods = ["OctopusColumnText", "OctopusSize", "TableUnionSearch", "WTStitching"]#, "$U_{NL}$", "$U_{Set}$", "$U_{Sem}$", "$U_{SemSet}$"]
querynum = 300
k = 65#25
#
#answeredQueries = get_answered_queries(dbs, tables)
#
ps = []
rs = []
for i in range(len(dbs)):
    if i>3:
        continue
    p, r = get_precision_recall(os.path.join(benchmark, dbs[i]), tables[i], querynum, k)
    ps.append(p)
    rs.append(r)
print(ps)
print(rs)
#
fs = 10
dist = 0.05
#cs = ['g', 'b','r','m','k','navy', 'y', 'b']
cs = ['royalblue', 'royalblue','royalblue','royalblue']
fig, ax = plt.subplots(1, 1, figsize=(3, 3))
#ax.set_title('Att and Table Union Search', fontsize=fs)
ax.tick_params(axis='both', which='major', labelsize=fs)
ax.grid(linestyle='dotted')
ax.set_xlabel('Recall', fontsize=fs)
ax.set_ylabel('Precision', fontsize=fs)
ax.set_xlim([0,1])
ax.set_ylim([0,1])
ax.set_xticks([0.0,0.2,0.4,0.6,0.8,1.0])
ax.set_yticks([0.0,0.2,0.4,0.6,0.8,1.0])
ax.set_axisbelow(True)
for i in range(len(dbs)):
    if i>3:
        continue
    print(dbs[i])
    plt.scatter(rs[i], ps[i], color=cs[i], edgecolor = 'black', marker='o', s=10**2, label = methods[i])
    if i == 0:
        ax.annotate(methods[i], (rs[i]+3*dist,ps[i]+dist), ha="right", va="bottom", fontsize=8)
    if i == 2:
        ax.annotate(methods[i], (rs[i]+2*dist,ps[i]+dist), ha="right", va="bottom", fontsize=8)
    if i == 1:
        ax.annotate(methods[i], (rs[i]+3.5*dist,ps[i]-2*dist), ha="right", va="bottom", fontsize=8)
    if i == 3:
        ax.annotate(methods[i], (rs[i]+3.5*dist,ps[i]+2*dist), ha="right", va="top", fontsize=8)
    if i == 4:
        ax.annotate(methods[i], (rs[i]-dist,ps[i]), ha="right", va="top", fontsize=8)
    if i == 5:
        ax.annotate(methods[i], (rs[i]-dist,ps[i]), ha="right", va="top", fontsize=8)
    if i == 6:
        ax.annotate(methods[i], (rs[i]-dist,ps[i]), ha="right", va="top", fontsize=8)
plt.tight_layout()
fig.subplots_adjust(hspace=0.2)
plt.savefig(args.output, bbox_inches='tight', pad_inches=0.02)
plt.close()
#
print("Done.")
