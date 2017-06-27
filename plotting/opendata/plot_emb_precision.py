import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3


parser = argparse.ArgumentParser()
parser.add_argument("-dbname", "--dbname", default="/home/fnargesian/TABLE_UNION_OUTPUT/experiments-fixedn.sqlite")
parser.add_argument("-output", "--output", default="plots/emb-cosine-fixed-n.pdf")
args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.dbname)
cursor = db.cursor()
ks = [3,5,10,20,26,45]
# for each k, we store query tables and their corresponding precision
queries = []
precisions = []
cs = ['b','g','r','c','m','y','k','orchid']
plt.figure(figsize=(18, 18))
plt.xlabel('Query Tables', fontsize=24)
plt.ylabel('Precision', fontsize=24)
plt.title('Precision of Cosine-emb for top-n results evaluated by a rule-based evaluater.', fontsize=24)
legends = []
table_num = 0
for k in ks:
    qs = []
    ps = []
    for query_table, query_precision in cursor.execute("select query_table, sum(correct)/(1.0*count(candidate_table)) as precision from (select query_table, candidate_table, count(*)/(1.0*?) as correct from fixedn_scores where k=? and query_col_name like candidate_col_name group by query_table, candidate_table, n) group by query_table order by precision desc;", (k,k)).fetchall():
        qs.append(query_table)
        ps.append(query_precision)
    if table_num < len(qs):
        table_num = len(qs)
    queries.append(qs)
    precisions.append(ps)
    print("Number of query tables: %d" % len(qs))
print("Plotting...")
for i in range(len(ks)):
    xs = [j for j in range(table_num)]
    # adding tail queries before plotting
    ps = precisions[i]
    for j in range(len(queries[i]), table_num):
        ps.append(0.0)
    print("len(ps): %d" % len(ps))
    l1 = plt.plot(xs, ps, color=cs[i], label='k = ' + str(ks[i]) + '(emb-cosine)')
    legends.append(l1[0])

plt.legend(handles=legends, fontsize='xx-large')
plt.savefig(args.output)
print("Done.")
