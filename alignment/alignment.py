import pandas as pd
import numpy as np
import networkx as nx
import requests                                                                                                                  
import networkx as nx
import sqlite3
from itertools import groupby
import json

domain_dir = '/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only'

def sample_cand(table, col):
    f = domain_dir + table
    df = pd.read_csv(f, sep=',', low_memory=False, encoding = "ISO-8859-1")
    cols = df.columns
    return list(df[col])[:min(5, len(list(df[col])))]

def sample_query(table, col):                                                                                                   
    f = table
    df = pd.read_csv(f, sep=',', low_memory=False, encoding = "ISO-8859-1")
    cols = df.columns
    return list(df[col])[:min(5, len(list(df[col])))]

def bipartite_graph(t1, t2):
    if t1 == t2:
        return {}
    edges = {}
    nodes1 = []
    nodes2 = []
    f1 = t1
    f2 = domain_dir + t2
    df1 = pd.read_csv(f1, sep=',', low_memory=False, encoding = "ISO-8859-1")
    df2 = pd.read_csv(f2, sep=',', low_memory=False, encoding = "ISO-8859-1")   
    cols1 = df1.columns
    cols2 = df2.columns
    num_cols1 = list(df1.select_dtypes(include=[np.number]).columns)
    cat_cols1 = list(set(cols1) - set(num_cols1))
    num_cols2 = list(df2.select_dtypes(include=[np.number]).columns)
    cat_cols2 = list(set(cols2) - set(num_cols2))
    for c1 in cat_cols1:
        nodes1.append(c1)
        dom1 = df1[c1].astype(str).tolist()
        vec1 = sum_emb(dom1)
        edges[c1] = {}
        for c2 in cat_cols2:
            nodes2.append(c2)
            dom2 = df2[c2].astype(str).tolist()
            vec2 = sum_emb(dom2)
            w = cosine(vec1, vec2)
            edges[c1][c2] = w
    return nodes1, nodes2, edges

def candidates(query):
    url = 'http://localhost:4003/query'
    df = pd.read_csv(query, sep=',', low_memory=False, encoding = 'ISO-8859-1')
    cols = df.columns
    cands = []
    num_cols = list(df.select_dtypes(include=[np.number]).columns)
    cat_cols = list(set(cols) - set(num_cols))
    for c in cat_cols:
        dom = df[c].astype(str).tolist()
        vec = sum_emb(dom)
        data = {'vec': vec, 'k': 5}
        response = requests.post(url, data=json.dumps(data), timeout=5)
        if response.ok:
            rdata = json.loads(response.content)
            results = rdata['result']
            for res in results:
                cands.append(res['table_id'])
        else:
            print("response not ok!")
            print("number of candidate tables is %d." % len(cand_tables))
    return cands

def sum_emb(values):                                                                                                         
    db = sqlite3.connect("/home/kenpu/datasets/FASTTEXT/fasttext.sqlite3")
    cursor = db.cursor()
    s = np.sum(fasttext_cells(cursor, values), axis=0)
    return s.tolist()

def match(query, cand, graph):
    print(cand)
    nodes1, nodes2, w_edges = graph(query, cand)
    M = nx.Graph()
    M.add_nodes_from(nodes1, bipartite=0)
    M.add_nodes_from(nodes2, bipartite=1)
    edges = []
    for e, vs in w_edges.items():
        for v, w in vs.items():
            edges.append((e,v,w))
    M.add_weighted_edges_from(edges)
    matching = nx.bipartite.maximum_matching(M)
    return matching, nodes1, nodes2, w_edges

def cosine(af1, af2):                                                                                                            
        return np.dot(af1, af2) / np.linalg.norm(af1) / np.linalg.norm(af2)

def fasttext_cells(c, cells):                                                                                                    
    features = []
    for cell in cells:
        f = fasttext_cell(c, cell)
        if f is not None:
            features.append(f)
    return np.array(features)

def fasttext_cell(c, cell):                                                                                                      
    feature = None
    for f in fasttext(c, cell):
        if feature is None:
            feature = f
        else:
            feature = feature + f
    return feature

def fasttext(c, values):
    ins = {}
    for key, group in groupby(values):
        ins[key] = len(list(group))
    qmarks = ",".join("?" for x in ins.keys())
    sql = "select word, emb from fasttext where word in (%s)" % qmarks
    c.execute(sql, tuple(ins.keys()))
    dt = np.dtype(float)
    dt = dt.newbyteorder('>')
    features = []
    for row in c.fetchall():
        word = row[0]
        emb_blob = row[1]
        emb_vec = np.frombuffer(emb_blob, dtype=dt)
        features.append(emb_vec)
    return features
