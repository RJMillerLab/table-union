import sqlite3
import os

def get_query_recall(dbname):
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    queries = {}
    print(dbname)
    for q, c in cursor.execute("select query_table, unionable_count from recall_groundtruth;").fetchall():
        queries[q] = c
    return queries


def get_measure_accuracy(dbname, tablename, query_count):
    print(tablename)
    queries = {}
    db = sqlite3.connect(dbname)
    cursor = db.cursor()
    query_sum_precision = {}
    #
    cursor.execute("create index if not exists " + tablename +"_inx1 on " + tablename + "(query_table, candidate_table);")
    cursor.execute("create index if not exists " + tablename +"_inx2 on " + tablename + "(table_percentile_plus);")
    cursor.execute("Drop table if exists scores_c;")
    cursor.execute("create table scores_c as select p.query_table as query_table, p.candidate_table as candidate_table, count(distinct p.query_col_name) as c from " + tablename + " p natural join (select query_table, candidate_table, query_col_name, candidate_col_name from all_groundtruth) g group by p.query_table, p.candidate_table;")
    cursor.execute("create index scinx1 on scores_c(query_table);")
    cursor.execute("create index scinx2 on scores_c(candidate_table);")
    #cursor.execute("drop table if exists scores_c_ordered;")
    #cursor.execute("create table scores_c_ordered as select sc.query_table as query_table, sc.candidate_table as candidate_table, sc.c as c, a.tpp as score from (select query_table, candidate_table, max(table_percentile_plus) as tpp from " + tablename + " group by query_table, candidate_table) a natural join scores_c sc order by a.query_table, a.tpp asc;")
    cursor.execute("drop table if exists scores_c_rank;")
    #cursor.execute("create table scores_c_rank as select a.query_table, a.candidate_table, a.c as c, a.score as score, (select count(*) from scores_c_ordered b where b.query_table=a.query_table and b._rowid_>a._rowid_)+1 as n from scores_c_ordered a;")
    cursor.execute("create  table scores_c_rank as select s.query_table as query_table, s.candidate_table as  candidate_table, s.c as c, a.n as n from scores_c s natural join (select distinct query_table,       candidate_table, n from " + tablename +") a;")
    cursor.execute("create index scrinx1 on scores_c_rank(query_table, candidate_table, c);")
    cursor.execute("create index scrinx2 on scores_c_rank(n);")
    #
    pstmt = "select avg(query_precision) as precision from (Select a.query_table as query_table, 1.0*count(a.candidate_table)/min((select min(60,count(distinct candidate_table)) from " + tablename + " where query_table=a.query_table), (select unionable_count from recall_groundtruth where query_table=a.query_table)) as query_precision from"
    pstmt += " (select distinct s.query_table as query_table, s.candidate_table as candidate_table from scores_c s, groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.           candidate_table and s.c>=g.c) a group by a.query_table);"
    for p in cursor.execute(pstmt).fetchall():
        print("precision: ")
        print(p[0])
    #
    for r in cursor.execute("select avg(query_recall) as recall from (Select a.query_table as query_table, 1.0*count(a.candidate_table)/min(60, (select unionable_count from recall_groundtruth where query_table=a.query_table)) as query_recall from (select distinct s.query_table as query_table, s.candidate_table as candidate_table from scores_c s, groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c>=g.c) a group by a.query_table);").fetchall():
        print("recall1: ")
        print(r[0])
    #
    for n in ns:
        correct = {}
        returned = {}
        change_in_recall = {}
        for query_table, count in cursor.execute("select s.query_table, count(s.candidate_table) as correct from  (select query_table, candidate_table, c from scores_c_rank where n<=" + str(n-1) + ") s, groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c>=g.c group by s.query_table;").fetchall():
            if query_table not in queries:
                queries[query_table] = 1
            correct[query_table] = count
            change_in_recall[query_table] = 0
            for query_table, count in cursor.execute("select s.query_table, count(s.candidate_table) as correct   from (select query_table, candidate_table, c from scores_c_rank where n=" + str(n-1) + " and          query_table='" + query_table + "') s, groundtruth_c g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c>=g.c group by s.query_table;").fetchall():
                if count > 0: #==1
                    change_in_recall[query_table] = 1
        for query_table, total in cursor.execute("select query_table, count(candidate_table) as total from (select  query_table, candidate_table, c from scores_c_rank where n<=" + str(n-1) + ") group by query_table;   ").fetchall():
            returned[query_table] = total
        for q, s in returned.items():
            if q not in correct:
                continue
            if q in query_sum_precision:
                query_sum_precision[q] += ((float(correct[q])/float(s)) * (float(change_in_recall[q])))
            else:
                query_sum_precision[q] = ((float(correct[q])/float(s)) * (float(change_in_recall[q])))
    average_precision = 0.0
    query_correct = {}
    for query_table, count in cursor.execute("select a.query_table, count(a.candidate_table) as correct   from  (select s.query_table as query_table, s.candidate_table as candidate_table, s.c as c from scores_c_rank s, groundtruth_c  g where s.query_table=g.query_table and s.candidate_table=g.candidate_table and s.c>=g.c) a group by query_table;").fetchall():
        query_correct[query_table] = count
    for q, sap in query_sum_precision.items():
        average_precision += (sap/float(query_correct[q]))#min(query_count[q], len(ns))))
    #mean_average_precision = average_precision /float(len(query_sum_precision))
    print("len(query_sum_precision):")
    print(len(query_sum_precision))
    mean_average_precision = average_precision / 1000.0
    print("map1: ")
    print(average_precision / 1000.0)
    print("map2: ")
    print(average_precision /float(len(query_sum_precision)))
    return mean_average_precision

#
dbs = ["tablesearch_results.sqlite", "tablesearch_results.sqlite", "tablesearch_results.sqlite", "tablesearch_results.sqlite"]#, "tablesearch_results.sqlite"]
tables = ["setnlsem_scores", "nl_scores", "set_scores", "sem_scores"]#,"semset_scores"]
benchmark = "/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v7"
methods = ["$U_{Ensemble}$", "$U_{NL}$", "$U_{Set}$", "$U_{Sem}$", "$U_{SemSet}$"]
querynum = 300
k = 60
#
query_count = query_counts = get_query_recall(os.path.join(benchmark, dbs[0]))
ns = [i for i in range(1,k+1)]
maps = []
for i in range(len(dbs)):
    m_a_p = get_measure_accuracy(os.path.join(benchmark, dbs[i]),tables[i], query_count)
    maps.append(m_a_p)
print("Done.")


