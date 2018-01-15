import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("-dbname", "--dbname", default="/home/fnargesian/TABLE_UNION_OUTPUT/attstats3.sqlite")
parser.add_argument("-outputa", "--outputa", default="plots/perc_buck_all_att")
args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.dbname)
cursor = db.cursor()
measures = ["set", "sem", "semset", "nl"]
tables = {}
tables["set"] = "att_bucket_global_perc_set"
tables["sem"] = "att_bucket_global_perc_sem"
tables["semset"] = "att_bucket_global_perc_semset"
tables["nl"] = "att_bucket_global_perc_nl"
#fig, axarr = plt.subplots(2, 2, figsize=(6, 6))
#axmap = {}
#axmap["set"] = axarr[0,0]
#axmap["nl"] = axarr[0,1]
#axmap["sem"] = axarr[1,0]
#axmap["semset"] = axarr[1,1]
for m in measures:
    fig, ax = plt.subplots(figsize=(3, 3))
    #ax = axmap[m]
    ax.grid(linestyle='dotted')
    ax.set_xlabel('LSH Bucket Percentile')#, fontsize=18)
    ax.set_ylabel('Global Percentile')#, fontsize=18)
    ax.set_title('$U_{'+m+'}$')
    global_ps = []
    buck_ps = []
    global_rs = []
    buck_rs = []
    xs = []
    tablename = tables[m]
    i = 0
    for gp, bp, gr, br in cursor.execute("select global_perc, buck_perc, global_rank, buck_rank from " + tablename + " where buck_perc>0.0;").fetchall():
        global_ps.append(gp)
        buck_ps.append(bp)
        global_rs.append(gr)
        buck_rs.append(br)
        xs.append(i)
        i += 1
    #ax.set_xticks(np.arange(min(buck_ps), max(buck_ps), (max(buck_ps)- min(buck_ps))/5))
    #ax.set_yticks(np.arange(min(global_ps), max(global_ps), (max(global_ps)- min(global_ps))/5))
    ax.set_ylim([min(global_ps),1.0])
    ax.set_xlim([min(buck_ps),1.0])
    print("Number of pairs: %d" % len(xs))
    print("Plotting...")
    ax.scatter(buck_ps, global_ps, label='LSH Bucket vs. Global Percentile', rasterized=True, marker='o', alpha=0.2, color='navy')
    # color='royalblue'
    plt.tight_layout()
#st.set_y(0.93)
    fig.subplots_adjust(hspace=0.2)
    print(args.outputa+"_"+m+".pdf")
    plt.savefig(args.outputa+"_"+m+".pdf", bbox_inches='tight')#, pad_inches=0.02)
#
