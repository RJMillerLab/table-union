import emb
import os
from sklearn.manifold import TSNE
import sqlite3
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
#from sklearn.decomposition import PCA
import matplotlib.patches as mpatches

root = "./domains"
domains = sorted(os.listdir(root))

result = []
db = sqlite3.connect("/home/kenpu/datasets/FASTTEXT/fasttext.sqlite3")
cursor = db.cursor()
#f1 = "scifi-movies-2000s-directors"
#f2 = "scifi-movies-2010s-directors"
#f1 = "nsf_name"
#f2 = "nserc_name"
f1 = "nserc_researchsubjectgroup"
#f1 = "nserc_researchsubjectgroup"
#f1 = "nserc_department"
f2 = "nsf_fieldofstudy"
f3 = "nserc_institution"
#f2 = "nsf_institution"
#f3 = "nserc_name"
#f3 = "fantasy-novels"
f3 = "universities"
#f2 = "everything"
d1 = os.path.join(root, f1 + ".csv")
d2 = os.path.join(root, f2 + ".csv")
d3 = os.path.join(root, f3 + ".csv")
p1 = os.path.join(root, d1)
p2 = os.path.join(root, d2)
p3 = os.path.join(root, d3)
e1 = emb.fasttext_cells(cursor, emb.get_domain_cells(d1))
e2 = emb.fasttext_cells(cursor, emb.get_domain_cells(d2))
e3 = emb.fasttext_cells(cursor, emb.get_domain_cells(d3))
#
print(emb.get_jaccard(d1,d2))
#
tsne = TSNE(perplexity=30, n_components=2, init='pca', n_iter=10000, random_state=0)
a = []
a.extend(e1)
a.extend(e2)
a.extend(e3)
ta = tsne.fit_transform(a)
with open("data.txt", "w") as f:
    f.write("%d %d %d\n" % (len(e1), len(e2), len(e3)))
    for t in ta:
        f.write("%f,%f\n" % (t[0], t[1]))

t3 = ta[len(e1)+len(e2):]
t2 = ta[len(e1):len(e1)+len(e2)]
t1 = ta[:len(e1)]
#
plt.figure(figsize=(18, 18))
ax = plt.axes()
x = []
y = []
xmean = 0
ymean = 0
for t in t1:
    x.append(t[0])
    y.append(t[1])
    xmean += t[0]
    ymean += t[1]
plt.scatter(x, y, color="r", alpha=0.5, marker='o', edgecolor='b', rasterized=True)
ax.arrow(0, 0, xmean/float(len(x)), ymean/float(len(x)), fc = 'r', ec='r', head_width=0.5, head_length=1)
#
x = []
y = []
xmean = 0
ymean = 0
for t in t2:
    x.append(t[0])
    y.append(t[1])
    xmean += t[0]
    ymean += t[1]
plt.scatter(x, y, color="b", alpha=0.5, marker='o', edgecolor='b', rasterized=True)
ax.arrow(0, 0, xmean/float(len(x)), ymean/float(len(x)), head_width=0.5, head_length=1, fc='b', ec='b')
#
x = []
y = []
xmean = 0
ymean = 0
for t in t3:
    x.append(t[0])
    y.append(t[1])
    xmean += t[0]
    ymean += t[1]
plt.scatter(x, y, color="y", alpha=0.5, marker='x', edgecolor='black', rasterized=True)
ax.arrow(0, 0, xmean/float(len(x)), ymean/float(len(x)), head_width=0.5, head_length=1,  fc='black', ec='black')
classes = [f1,f2, f3]
class_colours = ['r','b', 'y']
recs = []
for i in range(0,len(class_colours)):
        recs.append(mpatches.Rectangle((0,0),1,1,fc=class_colours[i]))
        plt.legend(recs,classes,loc=4)
plt.savefig("nl_domain.pdf")
print("done!")
