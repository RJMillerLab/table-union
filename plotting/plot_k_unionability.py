import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import json
import numpy as np
import os

cs = ['b','g','r','c','m','y','k','orchid']
ks = [2,3,4]
plt.figure(figsize=(18, 18))
plt.xlabel('Tables', fontsize=24)
plt.ylabel('Number of K-unionable Tables (log scale)', fontsize=24)
plt.title('Soft K-unionability in Ontology and Embedding Space', fontsize=24)
legends = []
for i in range(len(ks)):
    m1 = {}
    m2 = {}
    with open('testdata/' + str(ks[i]) + '_soft_unionable_ont.json') as fp:
        m1 = json.load(fp)
    with open('testdata/' + str(ks[i]) + '_soft_unionable_emb.json') as fp:
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
    #e_unionable = list(e_unionable[ts])
    xs = [j for j in range(len(o_unionable))]
    ys = list(np.log(np.asarray(o_unionable)+1))
    #ys = list(np.asarray(o_unionable))
    #l1 = plt.scatter(xs, ys, color=cs[i*2], marker='x', llabel='k = ' + str(ks[i]) + '(ontology)')
    l1 = plt.plot(xs, ys, color=cs[i*2], label='k = ' + str(ks[i]) + '(ontology)')
    legends.append(l1[0])
    ts = np.argsort(e_unionable)[::-1]
    e_unionable = list(e_unionable[ts])
    ys = list(np.log(np.asarray(e_unionable)+1))
    #ys = list(np.asarray(e_unionable))
    l2 = plt.plot(xs, ys, color=cs[i*2+1], label='k = ' + str(ks[i]) + '(embedding)')
    #l2 = plt.scatter(xs, ys, color=cs[i*2+1], marker='o', label='k = ' + str(ks[i]) + '(embedding)')
    legends.append(l2[0])
plt.legend(handles=legends, fontsize='xx-large')
plt.savefig(os.path.join('plots', 'soft_unionability.pdf'))
#
plt.figure(figsize=(18, 18))
plt.xlabel('Tables', fontsize=24)
plt.ylabel('Number of K-unionable Tables (log scale)', fontsize=24)
plt.title('Hard K-unionability in Ontology and Embedding Space', fontsize=24)
legends = []
for i in range(len(ks)):
    m1 = {}
    m2 = {}
    with open('testdata/' + str(ks[i]) + '_hard_unionable_ont.json') as fp:
        m1 = json.load(fp)
    with open('testdata/' + str(ks[i]) + '_hard_unionable_emb.json') as fp:
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
    #e_unionable = list(e_unionable[ts])
    xs = [j for j in range(len(o_unionable))]
    ys = list(np.log(np.asarray(o_unionable)+1))
    #ys = list(np.asarray(o_unionable))
    #l1 = plt.scatter(xs, ys, color=cs[i*2], marker='x', llabel='k = ' + str(ks[i]) + '(ontology)')
    l1 = plt.plot(xs, ys, color=cs[i*2], label='k = ' + str(ks[i]) + '(ontology)')
    legends.append(l1[0])
    ts = np.argsort(e_unionable)[::-1]
    e_unionable = list(e_unionable[ts])
    ys = list(np.log(np.asarray(e_unionable)+1))
    #ys = list(np.asarray(e_unionable))
    l2 = plt.plot(xs, ys, color=cs[i*2+1], label='k = ' + str(ks[i]) + '(embedding)')
    #l2 = plt.scatter(xs, ys, color=cs[i*2+1], marker='o', label='k = ' + str(ks[i]) + '(embedding)')
    legends.append(l2[0])
plt.legend(handles=legends, fontsize='xx-large')
plt.savefig(os.path.join('plots', 'hard_unionability.pdf'))
