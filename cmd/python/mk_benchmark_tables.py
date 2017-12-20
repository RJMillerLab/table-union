import argparse
import sys
import sqlite3
import random
import os

MIN_DISTINCT = 4
NUM_PROJ_PER_C = 2
NUM_BENCHMARK_TABLE_PER_BASE = 5

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Given a SQLite3 database of base tables,"
            "generate benchmark tables and store them in a new database.")
    parser.add_argument("-i", "--input", required=True,
            help="The SQLite3 database of the base tables")
    parser.add_argument("-o", "--output", required=True,
            help="The output SQLite3 database filename")
    args = parser.parse_args(sys.argv[1:])

    conn = sqlite3.connect(args.input)
    c = conn.cursor()

    tables = [r[0] for r in c.execute('''SELECT name FROM sqlite_master WHERE type == 'table';''')]

    # Get the text columns for each table
    columns = dict()
    for table in tables:
        cols = [r[1] for r in c.execute('''PRAGMA table_info({});'''.format(table))
                if r[2] == "TEXT"]
        columns[table] = cols

    # Get the distinct counts of columns
    counts = dict()
    for table in tables:
        counts[table] = dict()
        for column in columns[table]:
            counts[table][column] = c.execute('''SELECT count(distinct("{}")) FROM "{}";'''.format(column, table)).fetchone()[0]

    # Get number of rows of base tables
    nrows = dict()
    for table in tables:
        nrows[table] = c.execute('''SELECT count(rowid) FROM "{}";'''.format(table)).fetchone()[0]

    # Close connection to base table database
    conn.close()

    # Apply filter on distinct counts
    selected_columns = dict()
    for table in tables:
        selected_columns[table] = [column for column in columns[table]
                if counts[table][column] >= MIN_DISTINCT]

    # Generate random projections of columns
    projections = dict()
    for table in tables:
        projections[table] = dict()
        columns = selected_columns[table]
        for c in range(1, len(columns)):
            projections[table][c] = []
            for i in range(NUM_PROJ_PER_C):
                while True:
                    p = tuple(sorted(random.sample(columns, c)))
                    if p not in projections[table][c]:
                        break
                projections[table][c].append(p)
        projections[table][len(columns)] = [tuple(sorted(columns)),]

    # Create connection to the output database
    conn = sqlite3.connect(args.output)
    conn.execute('''ATTACH DATABASE "{}" AS base;'''.format(os.path.abspath(args.input)))

    # Create benchmark tables by selections
    for table in tables:
        # Do selection on all projections of this table
        for c in projections[table]:
            for i, columns in enumerate(projections[table][c]):

                # First create the deduplicated projected table
                proj_table_name = "{}____c{}_{}".format(table, c, i)
                fmt = ", ".join(['''"{}"'''.format(c) for c in columns])
                sql = '''CREATE TABLE "{}" AS SELECT DISTINCT {} FROM BASE."{}";'''.format(proj_table_name, fmt, table)
                conn.execute(sql)

                # Count the number of rows in the projected table
                # and set the limit and offsets
                nrow = conn.execute('''SELECT count(rowid) FROM "{}";'''.format(proj_table_name)).fetchone()[0]
                limit = int(nrow / NUM_BENCHMARK_TABLE_PER_BASE)
                offsets = [i*limit for i in range(NUM_BENCHMARK_TABLE_PER_BASE)]

                # Create the selections of the projected table
                for j, offset in enumerate(offsets):
                    name = "{}____c{}_{}____{}".format(table, c, i, j)
                    sql = '''CREATE TABLE "{}" AS SELECT * FROM "{}" LIMIT {} OFFSET {};'''.format(name, proj_table_name, limit, offset)
                    conn.execute(sql)

                # Drop the projected table after selections have created
                conn.execute('''DROP TABLE "{}";'''.format(proj_table_name))
    conn.commit()

    # Finish
    conn.close()
