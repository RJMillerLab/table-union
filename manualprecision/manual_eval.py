import os
import random
import sqlite3
import pandas as pd
import collections
import agate
import io

class Candidate:

    def __init__(self, query_table_name, candidate_table_name, query_table, candidate_table, alignment):
        self.query_table_name = query_table_name
        self.candidate_table_name = candidate_table_name
        self.query_table = query_table
        self.candidate_table = candidate_table
        cols = list(self.query_table)
        self.query_columns = [cols[x[0]] for x in alignment]
        cols = list(self.candidate_table)
        self.candidate_columns = [cols[x[1]] for x in alignment]

    def _print_table(self, table):
        with io.StringIO(table.to_csv(None, index=False)) as f:
            agate.Table.from_csv(f).print_table(max_rows=None, max_columns=None, max_column_width=40)

    def preview(self, s):
        print("Query Table: ")
        self._print_table(self.query_table[self.query_columns].sample(s))
        print("Candidate Table: ")
        self._print_table(self.candidate_table[self.candidate_columns].sample(s))

    def show_columns(self):
        def print_columns(df):
            for i, c in enumerate(list(df)):
                print("%d:\t%s" % (i, c))
        print("Query Table Columns: ")
        print_columns(self.query_table)
        print("Candidate Table Columns: ")
        print_columns(self.candidate_table)

    def preview_query(self, s):
        q = self.query_table.sample(s)
        self._print_table(q)

    def preview_candidate(self, s):
        c = self.candidate_table.sample(s)
        self._print_table(c)

class CandidateFactory:

    def __init__(self, path_prefix, result_sqlite, evaluation):
        self.path_prefix = path_prefix
        self.results = collections.defaultdict(lambda : collections.defaultdict(list))
        self.conn = sqlite3.connect(result_sqlite)
        self.conn.row_factory = lambda cursor, row : dict((col[0], row[i]) for i, col in enumerate(cursor.description))
        self.evaluation = evaluation

    def _read_table(self, table_name):
        filename = os.path.join(self.path_prefix, table_name)
        with open(filename) as f:
            n = sum(1 for _ in f)
        s = 2000
        skip = sorted(random.sample(range(1, n+1), max(n-s, 0)))
        return pd.read_csv(filename, skiprows=skip)

    def _get_alignments(self):
        prev_query_table_name = None
        prev_candidate_table_name = None
        alignment = []
        c = self.conn.cursor()
        rows = c.execute("SELECT * FROM debug WHERE n <= 5 ORDER BY query_table, candidate_table;")
        for row in rows:
            # Set local varaibles from database
            query_table_name = row['query_table']
            candidate_table_name = row['candidate_table']
            query_col_index = row['query_col_index']
            candidate_col_index = row['candidate_col_index']
            pair = (query_col_index, candidate_col_index)
            if (query_table_name == prev_query_table_name and candidate_table_name == prev_candidate_table_name) or (prev_query_table_name is None and prev_candidate_table_name is None):
                pass
            else:
                yield (prev_query_table_name, prev_candidate_table_name, alignment)
                alignment = []
            if pair not in alignment:
                alignment.append(pair)
            prev_query_table_name = query_table_name
            prev_candidate_table_name = candidate_table_name
        self.conn.close()

    def iterator(self):
        # Set loop shared variables
        query_table = None
        prev_query_table_name = None
        for query_table_name, candidate_table_name, alignment in self._get_alignments():
            # Skip if evaulated
            if self.evaluation.is_evaluated(query_table_name, candidate_table_name):
                continue
            # Set query table
            if query_table is None or query_table_name != prev_query_table_name:
                query_table = self._read_table(query_table_name)
            # Set candidate table
            candidate_table = self._read_table(candidate_table_name)
            # Update loop shared variables
            prev_query_table_name = query_table_name
            yield Candidate(query_table_name, candidate_table_name, query_table, candidate_table, alignment)


class Evaluation:

    def __init__(self, output_sqlite):
        self.conn = sqlite3.connect(output_sqlite)
        self.c = self.conn.cursor()
        self.c.execute('''
            CREATE TABLE IF NOT EXISTS manual_eval(
                query_table TEXT,
                candidate_table TEXT,
                is_correct BOOL,
                UNIQUE(query_table, candidate_table) ON CONFLICT REPLACE
            );''')
        self.conn.commit()

    def done(self):
        self.conn.commit()
        self.conn.close()

    def is_evaluated(self, query_table_name, candidate_table_name):
        self.c.execute("SELECT rowid FROM manual_eval WHERE query_table = ? AND candidate_table = ?;", (query_table_name, candidate_table_name))
        return self.c.fetchone() is not None

    def set(self, canadiate, is_correct):
        self.c.execute("INSERT INTO manual_eval VALUES (?, ?, ?);",
                (canadiate.query_table_name,
                 canadiate.candidate_table_name,
                 is_correct))
        self.conn.commit()

evaluation = Evaluation('./manual_eval.sqlite')
iterator = CandidateFactory('/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only', './canada_query_results.sqlite', evaluation).iterator()
candidate = None

def get():
    global candidate
    candidate = next(iterator)

def show(s=20):
    global candidate
    if candidate is None:
        print("Call get() first to get a candidate")
    candidate.preview(s)

def columns():
    global candidate
    if candidate is None:
        print("Call get() first to get a candidate")
    candidate.show_columns()

def q(s=20):
    global candidate
    if candidate is None:
        print("Call get() first to get a candidate")
    candidate.preview_query(s)

def x(s=20):
    global candidate
    if candidate is None:
        print("Call get() first to get a candidate")
    candidate.preview_candidate(s)

def yes():
    global candidate
    global evaluation
    if candidate is None:
        print("Call get() first to get a candidate")
    evaluation.set(candidate, True)

def no():
    global candidate
    global evaluation
    if candidate is None:
        print("Call get() first to get a candidate")
    evaluation.set(candidate, False)
