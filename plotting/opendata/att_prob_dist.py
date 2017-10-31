import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import sqlite3
import numpy as np
#from scipy.interpolate import UnivariateSpline

parser = argparse.ArgumentParser()
parser.add_argument("-dbname", "--dbname", default="/home/fnargesian/TABLE_UNION_OUTPUT/benchmark-v4/attstats.sqlite")
parser.add_argument("-tablename", "--tablename", default="att_unionability")
parser.add_argument("-outputa", "--outputa", default="plots/att-prob-dist.pdf")
args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.dbname)
cursor = db.cursor()
tablename = args.tablename
fig, ax = plt.subplots(figsize=(10, 10))
ax.set_title('Distribution of Att Unionability in Benchmark (10K pairs)', fontsize=18)
ax.set_xlabel('Unionability', fontsize=18)
ax.set_ylabel('Probability', fontsize=18)
probs = []
ps = []
i = 0
for s in cursor.execute("select score from " + tablename + " where score > 0.0001 order by random() limit 10000;").fetchall():
    probs.append(s[0])
    ps.append(i)
    i += 1
print("Number of pairs: %d" % len(ps))
print("Plotting...")
#probs_rank = np.argsort(np.asarray(probs))[::-1][:len(probs)]
#xs = list(np.asarray(ps)[probs_rank])
#xs = ps
#ys = list(np.asarray(probs)[probs_rank])
n = int(len(ps)/10)
p, x = np.histogram(probs, bins=n) # bin it into n = N/10 bins
x = x[:-1] + (x[1] - x[0])/2   # convert bin edges to centers
p = [float(c) / float(10000) for c in p]
#f = UnivariateSpline(x, p, s=n)
#plt.plot(x, f(x))
plt.plot(x, p)
#ax.scatter(xs, ys, color='y', marker='o', edgecolor='black', alpha=0.5, rasterized=True)
plt.savefig(args.outputa)
#
