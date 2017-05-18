import time
import numpy as np
import sqlite3
from sklearn.decomposition import PCA, IncrementalPCA

def get_embs(db, n):
    data = []
    dt = np.dtype('float')
    dt = dt.newbyteorder('>')
    for row in db.cursor().execute("SELECT emb FROM fasttext LIMIT %d;" % n).fetchall():
        data.append(np.frombuffer(row[0], dtype=dt))
    return np.array(data)

def pca(matrix):
    start = time.time()
    PCA().fit(matrix)
    return time.time() - start

def ipca(matrix):
    start = time.time()
    IncrementalPCA().fit(matrix)
    return time.time() - start

db = sqlite3.connect("/home/ekzhu/FB_WORD_VEC/fasttext.db")
scales = [10, 100, 1000, 10000, 100000, 1000000]
#for scale in scales:
#    matrix = get_embs(db, scale)
#    dur = pca(matrix)
#    print("n = %d:\tPCA took %fs" % (scale, dur))
for scale in scales:
    matrix = get_embs(db, scale)
    dur = pca(matrix)
    print("n = %d:\tIPCA took %fs" % (scale, dur))
