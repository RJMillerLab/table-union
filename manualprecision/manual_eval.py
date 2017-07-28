import os
import sqlite3
import agate
import collections

class Candidate:

    def __init__(self, query_table_name, candidate_table_name, query_table, candidate_table, alignment):
        self.query_table_name = query_table_name
        self.candidate_table_name = candidate_table_name
        self.query_table = query_table
        self.candidate_table = candidate_table
        self.query_columns = [x[0] for x in alignment]
        self.candidate_columns = [x[1] for x in alignment]

    def preview(self):
        q = self.query_table.select(self.query_columns)
        c = self.candidate_table.select(self.candidate_columns)
        print("Query Table: ")
        q.print_table()
        print("Candidate Table: ")
        c.print_table()

class CandidateFactory:

    def __init__(self, path_prefix, result_sqlite, evaluation):
        self.path_prefix = path_prefix
        self.results = collections.defaultdict(lambda : collections.defaultdict(list))
        self.conn = sqlite3.connect(result_sqlite)
        self.conn.row_factory = lambda cursor, row : dict((col[0], row[i]) for i, col in enumerate(cursor.description))
        self.evaluation = evaluation

    def _read_table(self, table_name):
        tester = agate.TypeTester(limit=100)
        return agate.Table.from_csv(os.path.join(self.path_prefix, table_name), column_types=tester)

    def _get_alignments(self):
        prev_query_table_name = None
        prev_candidate_table_name = None
        alignment = []
        c = self.conn.cursor()
        rows = c.execute("SELECT * FROM debug ORDER BY query_table, candidate_table;")
        for row in rows:
            # Set local varaibles from database
            query_table_name = row['query_table']
            candidate_table_name = row['candidate_table']
            query_col_name = row['query_col_name']
            candidate_col_name = row['candidate_col_name']
            if (query_table_name == prev_query_table_name and candidate_table_name == prev_candidate_table_name) or (prev_query_table_name is None and prev_candidate_table_name is None):
                alignment.append((query_col_name, candidate_col_name))
            else:
                yield (prev_query_table_name, prev_candidate_table_name, alignment)
                alignment = []
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
                is_correct BOOL
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
candidates = CandidateFactory('/home/ekzhu/OPENDATA/resource-2016-12-15-csv-only', './canada_query_results.sqlite', evaluation)

