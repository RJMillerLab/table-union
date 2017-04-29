import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import json
import numpy as np
import os


ks = [1,2,3,4,5]
for i in range(len(ks)):
    m1 = {}
    m2 = {}
    with open('testdata/' + str(ks[i]) + '_unionable_ontology.json') as fp:
        m1 = json.load(fp)
    with open('testdata/' + str(ks[i]) + '_unionable_emb.json') as fp:
        m2 = json.load(fp)
    o_unionable = []
    e_unionable = []
    for k, v in m1.items():
        o_unionable.append(len(v))
        e_unionable.append(len(m2[k]))
    o_unionable = np.asarray(o_unionable)
    e_unionable = np.asarray(e_unionable)
    ts = np.argsort(o_unionable)[::-1]
    o_unionable = list(o_unionable[ts])
    e_unionable = list(e_unionable[ts])
    #xs = [[i for i in range(len(o_unionable))], [i for i in range(len(o_unionable))]]
    #ys = [o_unionable, e_unionable]
    #cs = ['b', 'r']
    xs = [i for i in range(len(o_unionable))]
    ys = o_unionable
    cs = ['b']
    plt.figure(figsize=(18, 18))
    plt.xlabel('Tables', fontsize=24)
    plt.ylabel('Number of ' + str(ks[i]) + '-unionable Tables', fontsize=24)
    plt.title(str(ks[i]) + '-unionability in Ontology (blue) and Embedding (red) Space', fontsize=24)
    plt.scatter(xs, ys, color = cs)
    ys = e_unionable
    cs = ['r']
    plt.scatter(xs, ys, color = cs)
    plt.savefig(os.path.join('plots', str(ks[i]) + '_unionability.pdf'))
