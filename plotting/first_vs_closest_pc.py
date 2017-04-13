import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import collections
import os
import json

cosines = []
pcs_cosines = []
with open('testdata/cosines500.json', 'r') as fp:
    cosines = json.load(fp)
    cosines = cosines[:2000]
with open('testdata/pcs_cosines500.json', 'r') as cp:
    pcs_cosines = json.load(cp)
    pcs_cosines = pcs_cosines[:2000]
# plotting nearest pc cosines vs jaccards
print("Plotting...")
plt.figure(figsize=(18, 18))
plt.ylabel('nearest pc cosine', fontsize=24)
plt.xlabel('first pc cosine', fontsize=24)
plt.title('nearest (out of max 3 pcs) vs first pc cosine of column pairs', fontsize=24)
plt.scatter(cosines,pcs_cosines)
plt.savefig("first_vs_best_pcs.pdf")
print("Done.")
