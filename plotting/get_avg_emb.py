import argparse, sys
import numpy as np
import sqlite3

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataset", default="/home/fnargesian/WIKI_TABLE/search-index.db")
parser.add_argument("-t", "--tableid", default="")
parser.add_argument("-c", "--columnindex", default="")
args = parser.parse_args(sys.argv[1:])

db = sqlite3.connect(args.dataset)
cursor = db.cursor()
dt = np.dtype(float)
dt = dt.newbyteorder('>')
if args.tableid == "" or args.columnindex == "":
    for table_id, column_index,bin_vec in \
        cursor.execute("select table_id, column_index, avg_vec from search_index limit 5;").fetchall():
        vec = np.frombuffer(bin_vec, dtype=dt)
        print(vec)      
else:
    for table_id, column_index, bin_vec in \
        cursor.execute("select table_id, column_index, avg_vec from search_index where table_id=? and column_index=?;", (args.tableid, args.columnindex)).fetchall():
        vec = np.frombuffer(bin_vec, dtype=dt)
        print(vec)
print("Done.")
