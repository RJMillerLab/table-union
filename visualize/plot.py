import matplotlib.pyplot as pyplot
import numpy as np

with open("data.txt") as f:
    line = f.readline()
    (n1, n2, n3) = map(int, line.split())
    points = np.array([list(map(float, line.split(","))) for line in f.readlines()])
    P1 = points[0:n1, :]
    P2 = points[n1:n1+n2, :]
    P3 = points[n1+n2:, :]

def f(ax1):
    ax1.spines['left'].set_position('zero')
    ax1.spines['bottom'].set_position('zero')
    ax1.spines['right'].set_color('none')
    ax1.spines['top'].set_color('none')
    return ax1


pyplot.figure(figsize=(16,5))

P1 = P1[:100,:]
ax1 = f(pyplot.subplot(1,4,1))
pyplot.scatter(P2[:,0], P2[:,1], s=200, alpha=0.8, marker='^', color='b')

P2 = P2[:100,:]
ax2 = f(pyplot.subplot(1,4,2, sharex=ax1, sharey=ax1))
pyplot.scatter(P1[:,0], P1[:,1], s=200, alpha=0.8, marker='o', color='r')

P3 = P3[:100,:]
ax3 = f(pyplot.subplot(1,4,3, sharex=ax1, sharey=ax1))
pyplot.scatter(P3[:,0], P3[:,1], s=200, alpha=0.8, marker='.', color='g')

m1 = P1.mean(axis=0)
m2 = P2.mean(axis=0)
m3 = P3.mean(axis=0)


ax4 = f(pyplot.subplot(1,4,4, sharex=ax1, sharey=ax1))
pyplot.scatter(P1[:,0], P1[:,1], s=100, alpha=0.1, marker='o', color='r')
pyplot.scatter(P2[:,0], P2[:,1], s=40, alpha=0.1, marker='^', color='b')
pyplot.scatter(P3[:,0], P3[:,1], s=100, alpha=0.1, marker='.', color='g')
ax4.arrow(0, 0, m1[0], m1[1], head_width=2.5, head_length=4, fc='r', ec='r', width=1)
ax4.arrow(0, 0, m2[0], m2[1], head_width=2.5, head_length=4, fc='b', ec='b', width=1)
ax4.arrow(0, 0, m3[0], m3[1], head_width=2.5, head_length=4, fc='g', ec='g', width=1)


pyplot.savefig('a.pdf')
