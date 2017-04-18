import argparse, sys
import json
import os
import pandas as pd
import numpy as np


def get_domain(table_dir, table_id, column_id):
    df = pd.read_csv(os.path.join(table_dir, table_id))
    # the first two lines of csv files are headers and booleans for numerical columns
    col = np.array(df[df.columns[int(column_id)]])[1:]
    set_col = set(col)
    set_col.discard("")
    set_col.discard(np.nan)
    return set_col

def is_numerical(dom):
    col = {}
    col["a"] = dom
    df = pd.DataFrame.from_dict(col)
    if len(list(df.select_dtypes(include=[np.number]).columns)) > 0:
        return True
    else:
        return False


parser = argparse.ArgumentParser()
parser.add_argument("-fpd", "--firstpcdir", default="example_domains/first_pc")
parser.add_argument("-bpd", "--bestpcdir", default="example_domains/best_pc")
parser.add_argument("-jd", "--jaccarddir", default="example_domains/jaccard")
parser.add_argument("-wtd", "--wikitabledir", default="/home/ekzhu/WIKI_TABLE/tables")
args = parser.parse_args(sys.argv[1:])

pairs = []
first_cosines = []
best_cosines = []
jaccards = []
with open('testdata/pairs.json') as fp:
    pairs = json.load(fp)
with open('testdata/cosines.json') as fp:
    first_cosines = json.load(fp)
with open('testdata/pcs_cosines.json') as fp:
    best_cosines = json.load(fp)
with open('testdata/jaccards.json') as fp:
    jaccards = json.load(fp)

for i in range(len(pairs)):
    p = pairs[i]
    if i % 100 == 1:
        print("Processed %d domain pairs." % i)
    if jaccards[i] < 0.2:
        continue
    d1 = list(get_domain(args.wikitabledir, p[0], p[1]))
    d2 = list(get_domain(args.wikitabledir, p[2], p[3]))
    path = ""
    if best_cosines[i] > 0.5:
        path = os.path.join(os.path.join(args.bestpcdir, "cosine1"), p[0] + "_" + str(p[1]) + "_" + p[2] + "_" + str(p[3]))
    elif best_cosines[i] < -0.5:
        path = os.path.join(os.path.join(args.bestpcdir, "cosine_1"), p[0] + "_" + str(p[1]) + "_" + p[2] + "_" + str(p[3]))
    elif best_cosines[i] < 0.5 and best_cosines[i] > -0.5:
        path = os.path.join(os.path.join(args.bestpcdir, "cosine0"), p[0] + "_" + str(p[1]) + "_" + p[2] + "_" + str(p[3]))
    else:
        continue
    with open(path, 'w') as f:
        f.write("first_cosines: " + str(first_cosines[i]) + "\n")
        f.write("best_cosines: " + str(best_cosines[i]) + "\n")
        f.write("jaccards: " + str(jaccards[i]) + "\n")
        f.write("\nDomain1: \n")
        for a in d1:
            f.write(a + ",")
        f.write("\n\nDomain2: \n")
        for a in d2:
            f.write(a + ",")
    path = ""
    if first_cosines[i] > 0.25:
        path = os.path.join(os.path.join(args.firstpcdir, "cosine1"), p[0] + "_" + str(p[1]) + "_" + p[2] + "_" + str(p[3]))
    elif first_cosines[i] < -0.25:
        path = os.path.join(os.path.join(args.firstpcdir, "cosine_1"), p[0] + "_" + str(p[1]) + "_" + p[2] + "_" + str(p[3]))
    elif first_cosines[i] < 0.25 and best_cosines[i] > -0.25:
        path = os.path.join(os.path.join(args.firstpcdir, "cosine0"), p[0] + "_" + str(p[1]) + "_" + p[2] + "_" + str(p[3]))
    else:
        continue
    with open(path, 'w') as f:
        f.write("first_cosines: " + str(first_cosines[i]) + "\n")
        f.write("best_cosines: " + str(best_cosines[i]) + "\n")
        f.write("jaccards: " + str(jaccards[i]) + "\n")
        f.write("\nDomain1: \n")
        for a in d1:
            f.write(a + ",")
        f.write("\n\nDomain2: \n")
        for a in d2:
            f.write(a + ",")

    if jaccards[i] > 0.2:
        path = os.path.join(args.jaccarddir, p[0] + "_" + str(p[1]) + "_" + p[2] + "_" + str(p[3]))
        with open(path, 'w') as f:
            f.write("first_cosine: " + str(first_cosines[i]) + "\n")
            f.write("best_cosine: " + str(best_cosines[i]) + "\n")
            f.write("jaccard: " + str(jaccards[i]) + "\n")
            f.write("\nDomain1: \n")
            for a in d1:
                f.write(a + ",")
            f.write("\n\nDomain2: \n")
            for a in d2:
                f.write(a + ",")

print("Done.")
