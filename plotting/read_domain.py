import argparse, sys
import numpy as np
import sqlite3
import collections
import pandas as pd
import os
import json


def get_domain(table_dir, table_id, column_id):
    df = pd.read_csv(os.path.join(table_dir, table_id))
    # the first two lines of csv files are headers and booleans for numerical columns
    col = np.array(df[df.columns[int(column_id)]])[1:]
    set_col = set(col)
    set_col.discard("")
    set_col.discard(np.nan)
    return set_col


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--database", default="/home/fnargesian/WIKI_TABLE/search-index.db")
parser.add_argument("-t", "--tableid", default="plots/jaccard_best_pc.pdf")
parser.add_argument("-c", "--columnindex", default="plots/jaccard_first_pc.pdf")
parser.add_argument("-od", "--onttabledir", default="/home/fnargesian/WIKI_TABLE/onttables")
parser.add_argument("-td", "--tabledir", default="/home/ekzhu/WIKI_TABLE/tables")
args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.database)
cursor = db.cursor()
dt = np.dtype(float)
dt = dt.newbyteorder('>')
pos_count = 0
neg_count = 0
table_id = args.tableid
column_index = args.columnindex
print("domain values:")
print(get_domain(args.tabledir, table_id, column_index))
print("domain entity values:")
print(get_domain(args.onttabledir, table_id, column_index))
for bin_vec, pc_index in \
    cursor.execute("select vec, pc_index from search_index where table_id=? and column_index=?", (table_id, column_index)).fetchall():
    vec = np.frombuffer(bin_vec, dtype=dt)
    print("pc[%d]:" % pc_index)
    print(vec) 
print("Done.")
