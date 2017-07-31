import sqlite3
import numpy as np
import sys
import csv
import re
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from scipy.spatial.distance import euclidean

def get_domain_cells(dom_file):
    with open(dom_file, 'rb') as f:
        for line in f.readlines():
            line = line.decode('utf8', errors='ignore').strip().strip('"')
            yield [word.lower() for word in re.findall(r'\w+', line)]

def get_union_cells(dom_file1, dom_file2):
    u1 = list(get_domain_cells(dom_file1)) + list(get_domain_cells(dom_file2))
    u2 = set(tuple(x) for x in u1)
    union = [list(x) for x in u2]
    return union

def fasttext(c, values):
    qmarks = ",".join("?" for x in values)
    sql = "select word, emb from fasttext where word in (%s)" % qmarks
    c.execute(sql, values)
    dt = np.dtype(float)
    dt = dt.newbyteorder('>')
    features = []
    for row in c.fetchall():
        word = row[0]
        emb_blob = row[1]
        emb_vec = np.frombuffer(emb_blob, dtype=dt)
        features.append(emb_vec)
    return features

def fasttext_cell(c, cell):
    feature = None
    for f in fasttext(c, cell):
        if feature is None:
            feature = f
        else:
            feature = feature + f
    return feature

def fasttext_cells(c, cells):
    features = []
    for cell in cells:
        f = fasttext_cell(c, cell)
        if f is not None:
            features.append(f)
    return np.array(features)

def average(features):
    return np.array([np.average(features, axis=0)])

def sum(features):
    s = np.sum(features, axis=0)
    return np.array([s])

def pca(features):
    pca = PCA(n_components=3)
    p = pca.fit(features).components_
    return p[0]

def pcas(features):
    pca = PCA(n_components=3)
    p = pca.fit(features).components_
    return p

def kmeans(features):
    km = KMeans(n_clusters=3, random_state=0).fit(features)
    return km.cluster_centers_

def cosine(af1, af2):
    return np.dot(af1, af2) / np.linalg.norm(af1) / np.linalg.norm(af2)

def pcas_vars(features):
    pca = PCA(n_components=3)
    pca.fit(features)
    p = pca.components_
    v = pca.explained_variance_ratio_
    return p, v

def closest(F1, F2):
    max_sim = 0
    for x in F1:
        for y in F2:
            sim = cosine(x, y)
            if max_sim < sim: max_sim = sim
    return max_sim

def l2_closest(F1, F2):
    max_sim = 0
    for x in F1:
        for y in F2:
            sim = euclidean(x, y)
            if max_sim < sim: max_sim = sim
    return max_sim

def topic_closest(F1, F2, V1, V2):
    max_sim = 0
    for x in F1:
        for y in F2:
            sim = cosine(x, y)
            if max_sim < sim: max_sim = sim
    return max_sim

def get_jaccard(d1, d2):
    F1 = set(list(" ".join(ws) for ws in get_domain_cells(d1)))
    F2 = set(list(" ".join(ws) for ws in get_domain_cells(d2)))
    print("size(d1): %d and size(d2): %d and intersect(d1,d2): %d"% (len(list(F1)), len(list(F2)), len(F1.intersection(F2))))
    return float(len(F1.intersection(F2))) / len(F1.union(F2))

def get_pcas(domain):
    db = sqlite3.connect("/home/kenpu/datasets/FASTTEXT/fasttext.sqlite3")
    cursor = db.cursor()
    features = fasttext_cells(cursor, get_domain_cells(domain))
    pca = PCA(n_components=min(300, len(features)))
    pca.fit(features)
    p = pca.components_
    v = pca.explained_variance_ratio_
    return p, v

def get_similarity(d1, d2, aggr, compare):
    db = sqlite3.connect("/home/kenpu/datasets/FASTTEXT/fasttext.sqlite3")
    cursor = db.cursor()
    F1 = fasttext_cells(cursor, get_domain_cells(d1))
    F2 = fasttext_cells(cursor, get_domain_cells(d2))
    af1 = aggr(F1)
    af2 = aggr(F2)
    return compare(af1, af2)

def get_similarity_biased(d1, d2, aggr, compare):
    db = sqlite3.connect("/home/kenpu/datasets/FASTTEXT/fasttext.sqlite3")
    cursor = db.cursor()
    F1 = fasttext_cells(cursor, get_domain_cells(d1))
    F2 = fasttext_cells(cursor, get_domain_cells(d2))
    af1 = aggr(F1)
    af2 = aggr(F2)
    m1 = np.mean(F1, axis=0)
    m2 = np.mean(F2, axis=0)
    return compare(af1+m1, af2+m2)

def get_pcs_vars(d1):
    db = sqlite3.connect("/home/kenpu/datasets/FASTTEXT/fasttext.sqlite3")
    cursor = db.cursor()
    F1 = fasttext_cells(cursor, get_domain_cells(d1))
    return pcas_vars(F1)

def get_pcs_vars_union(d1, d2):
    db = sqlite3.connect("/home/kenpu/datasets/FASTTEXT/fasttext.sqlite3")
    cursor = db.cursor()
    F1 = fasttext_cells(cursor, get_union_cells(d1,d2))
    return pcas_vars(F1)

def get_features(d1):
    db = sqlite3.connect("/home/kenpu/datasets/FASTTEXT/fasttext.sqlite3")
    cursor = db.cursor()
    return fasttext_cells(cursor, get_domain_cells(d1))
