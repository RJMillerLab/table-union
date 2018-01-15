import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
plt.style.use("acm-2col.mplstyle")
import sqlite3
import os

def get_answered_queries(bpath, dbnames, tablenames):
    queries = {}
    for i in range(len(dbnames)):
        db = os.path.join(bpath, dbnames[i])
        table = tablenames[i]
        db = sqlite3.connect(db)
        cursor = db.cursor()
        for query_name in cursor.execute("select distinct query_table from " + table + ";").fetchall():
            queries[query_name] = 1
    return queries


def get_query_recall(dbname, k):
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    count = 0
    for c in cursor.execute("select count(query_table) from recall_groundtruth where unionable_count >=n;").fetchall():
        count = c[0]
    return count


def get_measure_accuracy(dbname, tablename):
    print(tablename)
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    ps = []
    rs = []
    #
    cursor.execute("create index if not exists " + tablename +"_inx1 on " + tablename + "(query_table, candidate_table);")
    cursor.execute("create index if not exists " + tablename +"_inx2 on " + tablename + "(table_percentile);")
    cursor.execute("Drop table if exists scores_c;")
    cursor.execute("create table scores_c as select p.query_table as query_table, p.candidate_table as candidate_table, count(distinct p.query_col_name) as c from " + tablename + " p natural join (select query_table, candidate_table, query_col_name, candidate_col_name from all_groundtruth) g group by p.query_table, p.candidate_table;")
    cursor.execute("create index scinx1 on scores_c(query_table);")
    cursor.execute("create index scinx2 on scores_c(candidate_table);")
    #cursor.execute("drop table if exists scores_c_ordered;")
    #cursor.execute("create table scores_c_ordered as select sc.query_table as query_table, sc.candidate_table as candidate_table, sc.c as c, a.tpp as score from (select query_table, candidate_table, max(table_percentile_plus) as tpp from " + tablename + " group by query_table, candidate_table) a natural join scores_c sc order by a.query_table, a.tpp asc;")
    cursor.execute("drop table if exists scores_c_rank;")
    #cursor.execute("create table scores_c_rank as select a.query_table, a.candidate_table, a.c as c, a.score  as score, (select count(*) from scores_c_ordered b where b.query_table=a.query_table and b._rowid_>a._rowid_) as n from scores_c_ordered a;")
    cursor.execute("create  table scores_c_rank as select s.query_table as query_table, s.candidate_table as candidate_table, s.c as c, a.n as n from scores_c s natural join (select distinct query_table, candidate_table, n from " + tablename +") a;")
    cursor.execute("create index scrinx1 on scores_c_rank(query_table, candidate_table, c);")
    cursor.execute("create index scrinx2 on scores_c_rank(n);")
    for n in ns:
        print(n)
        #correct = {}
        #returned = {}
        cursor.execute("drop table if exists scores_c_help;")
        cursor.execute("create table scores_c_help as select query_table, candidate_table, c from scores_c_rank where n<=" + str(n-1) + ";")
        for p in cursor.execute("select avg(query_precision) as precision from (Select a.query_table as query_table, 1.0*count(a.candidate_table)/(select count(distinct candidate_table) from scores_c_help where query_table=a.query_table) as query_precision from (select distinct s.query_table as query_table, s.candidate_table as candidate_table from scores_c_help s, groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c>=g.c) a group by a.query_table);").fetchall():
            precision = p[0]
        #for query_table, count in cursor.execute("select query_table, count(candidate_table) as correct from (select query_table, candidate_table, c from scores_c_rank where n<=" + str(n) + ") natural join groundtruth_c;").fetchall():
            #correct[query_table] = count
        #for query_table, total in cursor.execute("select query_table, count(*) as total from (select query_table, candidate_table from scores_c_rank where n<=" + str(n) + ") group by query_table;").fetchall():
        for r in cursor.execute("select sum(query_recall)/1000.0 as recall from (Select a.query_table as query_table, 1.0*count(a.candidate_table)/min(" + str(len(ns)) + ", (select unionable_count from recall_groundtruth where query_table=a.query_table)) as query_recall from (select distinct s.query_table as query_table, s.candidate_table as candidate_table from scores_c_help s, groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c>=g.c) a group by a.query_table);").fetchall():
            recall = r[0]
            #returned[query_table] = total
        #sumPrecision = 0.0
        #sumRecall = 0.0
        #for q, s in returned.items():
        #    if q in correct:
        #        sumPrecision += (float(correct[q])/float(s))
        #        sumRecall += (float(correct[q])/float(len(ns)))
        #query_count = get_query_recall(dbname, n)
        #ps.append(sumPrecision/float(len(returned)))
        ps.append(precision)
        #rs.append(sumRecall/float(len(queries)))
        #rs.append(sumRecall/float(query_count))
        rs.append(recall)
        if n == max(ns):
            print(n)
            print(precision)
            print(recall)
    print("Done precision...")
    return ps, rs


parser = argparse.ArgumentParser()
parser.add_argument("-outputa", "--outputa", default="plots/attable_5000_precision_k.pdf")
parser.add_argument("-outputb", "--outputb", default="plots/attable_5000_recall_k.pdf")
args = parser.parse_args(sys.argv[1:])

legends = []
all_ps = []
all_rs = []

k = 60
ns = [i for i in range(1,k+1)]

dbs = ["tablesearch_results.sqlite", "tablesearch_results.sqlite", "tablesearch_results.sqlite", "tablesearch_results.sqlite"]
tables = ["setnlsem_scores", "nl_scores", "set_scores", "sem_scores"]#"benchmark50001000"
#"semsetnl_scores",
benchmark = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v7"
methods = ["$U_{Ensemble}$", "$U_{NL}$", "$U_{Set}$", "$U_{Sem}$"]
#markers = ["x", "+", "*", "o", "-"]

#answeredQueries = get_answered_queries(benchmark, dbs, tables)

for i in range(len(dbs)):
    ps, rs = get_measure_accuracy(os.path.join(benchmark, dbs[i]),tables[i])#, answeredQueries)
    all_ps.append(ps)
    all_rs.append(rs)

fs = 11
dist = 0.05
#markersize = 2
lw = 2
linestyles = ["-+", "-", "--", "-x", "-."]
#cs = ['g','r','c','m','y','k','orchid']
cs = ['royalblue', 'purple', 'darkgreen', 'r', 'goldenrod']
fig, ax = plt.subplots(1, 1, figsize=(3, 3))
ax.tick_params(axis='both', which='major', labelsize=fs)
ax.grid(linestyle='dotted')
ax.set_xlabel('K', fontsize=fs)
ax.set_ylabel('Precision', fontsize=fs)
ax.set_xlim([1,k])
ax.set_ylim([0.2,1])
ax.set_axisbelow(True)
ax.tick_params(axis='both', which='major', labelsize=fs)
ax.tick_params(axis='both', which='major', labelsize=fs)
ticks = [1]
ticks.extend([i*10 for i in range(1,7)])
ax.set_xticks(ticks)
#ax.set_xticks([i*10 for i in range(7)])
ax.set_yticks([0.2,0.4,0.6,0.8,1.0])


for i in range(len(dbs)):
    print(dbs[i])
    ax.plot(ns, all_ps[i], linestyles[i], label=methods[i], color = cs[i], linewidth=lw, markevery=5)
lgd = plt.legend(ncol=2, loc="best", bbox_transform=plt.gcf().transFigure, fontsize=fs, fancybox=True, framealpha=0.5)
plt.savefig(args.outputa, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=0.02)
plt.close()
print("Done Plotting Precision.")
fig, ax = plt.subplots(1, 1, figsize=(3, 3))
ax.tick_params(axis='both', which='major', labelsize=fs)
ax.grid(linestyle='dotted')
ax.set_xlabel('K', fontsize=fs)
ax.set_ylabel('Recall', fontsize=fs)
ax.set_xlim([1,k])
ax.set_ylim([0,1])
ax.set_axisbelow(True)

ax.tick_params(axis='both', which='major', labelsize=fs)
ticks = [1]
ticks.extend([i*10 for i in range(1,7)])
ax.set_xticks(ticks)
ax.set_yticks([0.0,0.2,0.4,0.6,0.8,1.0])

for i in range(len(dbs)):
    ax.plot(ns, all_rs[i], linestyles[i], label=methods[i], color = cs[i], linewidth=lw, markevery=5)
lgd = plt.legend(ncol=1, loc="best", bbox_transform=plt.gcf().transFigure, fontsize=fs, fancybox=True, framealpha=0.5)
plt.savefig(args.outputb, bbox_extra_artists=(lgd,), bbox_inches='tight', pad_inches=0.02)
plt.close()
print("Done Plotting Recall.")
