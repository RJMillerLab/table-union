import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("-dbname", "--dbname", default="/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4/attstats.sqlite")
parser.add_argument("-tablename", "--tablename", default="all_att_unionability")
parser.add_argument("-outputa", "--outputa", default="plots/att-cdf.pdf")
args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.dbname)
cursor = db.cursor()
tablename = args.tablename
measures = ["set", "sem", "semset", "nl"]
fig, axarr = plt.subplots(2, 2, figsize=(10, 10))
axmap = {}
axmap["set"] = axarr[0,0]
axmap["nl"] = axarr[0,1]
axmap["sem"] = axarr[1,0]
axmap["semset"] = axarr[1,1]
for m in measures:
    ax = axmap[m]
    ax.set_title(m + '-Unionability CDF', fontsize=18)
    ax.set_xlabel(m +'-Unionability Score', fontsize=18)
    ax.set_ylabel('Accumulative Probability', fontsize=18)
    xs = []
    ys = []
    total = 0
    for s, c in cursor.execute("select score, count(*) as count from " + tablename + " where measure = '" + m + "' group by score order by score asc;").fetchall():
        xs.append(s)
        total += c
        if len(ys) == 0:
            ys.append(c)
        else:
            ys.append(ys[-1]+c)
    ys = [float(y)/float(total) for y in ys]
    print("Number of pairs: %d" % len(xs))
    print("Plotting...")
    ax.plot(xs, ys)
fig.subplots_adjust(hspace=0.3)
plt.savefig(args.outputa)
#
