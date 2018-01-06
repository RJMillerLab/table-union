import sqlite3
import argparse
import sys
import hashlib

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy tables from existing SQLite3 databases into a new one. Accepting '<database_filename> <table_name>' pairs as line-seperated from stdin")
    parser.add_argument("-o", "--output", required=True,
            help="Output SQLite3 database filename")

    args = parser.parse_args(sys.argv[1:])

    databases = dict()
    conn = sqlite3.connect(args.output)

    conn.execute('''CREATE TABLE metadata (table_name TEXT PRIMARY KEY, data BLOB);''')

    for line in sys.stdin:
        database, table = line.rstrip().split()
        if database not in databases:
            name = "t%s" % (hashlib.sha1(database.encode("utf-8")).hexdigest()[:8])
            databases[database] = name
            conn.execute('''ATTACH DATABASE "{}" AS "{}";'''.format(database, name))
        # Copy table
        conn.execute('''CREATE TABLE {0} AS SELECT * FROM "{1}"."{0}";'''.format(table,
            databases[database]))
        # Copy metadata
        d = conn.execute('''
            SELECT data FROM "{0}".metadata
            WHERE id = (SELECT metadata_id FROM "{0}".datafiles
                        WHERE table_name = '{1}');
            '''.format(databases[database], table)).fetchone()[0]
        conn.execute('''INSERT INTO metadata (table_name, data)
                        VALUES(?, ?)''', (table, d))

    conn.commit()
    conn.close()
