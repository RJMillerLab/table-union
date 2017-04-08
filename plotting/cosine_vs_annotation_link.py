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
for i, row in enumerate(cursor.execute("select vec1, vec2, label from benchmark;")):
    if None in row:
        print("Warning: Null in result %d" % i)
        continue
    vec1 = np.frombuffer(row[0], dtype=dt)
    vec2 = np.frombuffer(row[1], dtype=dt)
    cos = cosine(vec1, vec2)
    label = row[2]
    cosines.append(cos)
    labels.append(label)
cosines = np.array(list(cosines))
labels = np.array(list(labels))
print("Finish loading dataset")

plt.plot(labels, cosines, rasterized=True)
plt.tight_layout()
plt.savefig(args.output)
plt.close()
