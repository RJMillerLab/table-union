import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial.distance import cosine 
import sqlite3
import collections

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--labeled-dataset", default="/home/ekzhu/WWT/labeled-column-pairs.db")
parser.add_argument("-o", "--output", default="cosine_vs_annotation_link.pdf")
args = parser.parse_args(sys.argv[1:])

cosines = collections.deque([])
labels = collections.deque([])
db = sqlite3.connect(args.labeled_dataset)
cursor = db.cursor()
dt = np.dtype(float)
dt = dt.newbyteorder('>')
for table_id1, table_id2, column_index1, column_index2, label in \
        cursor.execute("select table_id1, table_id2, column_index1, column_index2, label from column_pair;"):
    bin_vec1 = cursor.execute("select vec from domain_vec where table_id=? and column_index=?", table_id1, column_index1).fetchone() 
    bin_vec2 = cursor.execute("select vec from domain_vec where table_id=? and column_index=?", table_id2, column_index2).fetchone()
    vec1 = np.frombuffer(bin_vec1, dtype=dt)
    vec2 = np.frombuffer(bin_vec2, dtype=dt)
    cos = cosine(vec1, vec2)
    cosines.append(cos)
    labels.append(label)
cosines = np.array(list(cosines))
labels = np.array(list(labels))
print("Finish loading dataset")

plt.plot(labels, cosines, rasterized=True)
plt.tight_layout()
plt.savefig(args.output)
plt.close()
print("Figure saved to %s" % args.output)
