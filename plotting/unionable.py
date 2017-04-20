import argparse, sys
import numpy as np
import sqlite3
import collections
import pandas as pd
import os
import json
import time
from scipy.spatial.distance import cosine

parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataset", default="/home/fnargesian/WIKI_TABLE/search-index.db")
parser.add_argument("-otd", "--onttabledir", default="/home/fnargesian/WIKI_TABLE/onttables")
parser.add_argument("-td", "--tabledir", default="/home/ekzhu/WIKI_TABLE/tables")
parser.add_argument("-yp", "--ontologyplus", default="/home/fnargesian/go/src/github.com/RJMillerLab/table-union/ontology/ancestors.yago")
parser.add_argument("-y", "--ontology", default="/home/fnargesian/go/src/github.com/RJMillerLab/table-union/ontology/types.yago") 
parser.add_argument("-di", "--domaininfo", default="/home/fnargesian/go/src/github.com/RJMillerLab/table-union/plotting/testdata/domain-db")
args = parser.parse_args(sys.argv[1:])

def get_domain(table_dir, table_id, column_id):
    df = pd.read_csv(os.path.join(table_dir, table_id))
    # the first two lines of csv files are headers and booleans for numerical columns
    col = np.array(df[df.columns[int(column_id)]].str.lower())[1:]
    set_col = set(col)
    set_col.discard("")
    set_col.discard(np.nan)
    return set_col

def jaccard_similarity(x,y):
    intersection_cardinality = len(set.intersection(*[x, y]))
    union_cardinality = len(set.union(*[x, y]))
    if union_cardinality > 0:
        jac = intersection_cardinality/float(union_cardinality)
    else:
        # empty domains
        jac = None
    return jac

# annotate_domain_plus annotates a domain with classes two levels up in the ontology
def annotate_domain_plus(dom):
    annotations = []
    unseen = 0
    for v in dom:
        if v in ontology_plus:
            annotations.extend(ontology_plus[v])
        else:
            unseen += 1
            #print("Entity %s cannot be found in the ontology." % v)
    if unseen > 0:
        print("unseen entities: %d out of %d" % (unseen, len(dom)+unseen))
    return set(annotations)

# annotate_domain annotates a domain with classes one level up in the ontology
def annotate_domain(dom):
    annotations = []
    unseen = 0
    for v in dom:
        if v in ontology:
            annotations.extend(ontology[v])
        else:
            unseen += 1
            #print("Entity %s cannot be found in the ontology." % v)
    if unseen > 0:
        print("unseen entities: %d out of %d" % (unseen, len(dom)+unseen))
    return set(annotations)


seen_pairs = []
pairs = []
best_pc_cosines = []
first_pc_cosines = []
value_jaccards = []
entity_jaccards = []
ontology_jaccards = []
ontology_plus_jaccards = []
db = sqlite3.connect(args.dataset)
cursor = db.cursor()
db2 = sqlite3.connect(args.domaininfo)
cursor2 = db2.cursor()
count = 0
dt = np.dtype(float)
dt = dt.newbyteorder('>')
# ontology init
ontology_plus = {}
ontology = {}
with open(args.ontologyplus, 'r') as of:
    ontology_plus = json.load(of)
with open(args.ontology, 'r') as of:
    ontology = json.load(of)
#
cursor2.execute("drop table if exists annotations;")
cursor2.execute("drop table if exists annotations_plus;")
cursor2.execute("drop table if exists scores;")
cursor2.execute("create table annotations (table_id text, column_index integer, class_annotation text);")
cursor2.execute("create table annotations_plus (table_id text, column_index integer, class_annotation text);")
cursor2.execute("create table scores (table_id1 text, column_index1 integer, table_id2 text, column_index2 integer, value_jaccard real, entity_jaccard real, ontology_jaccard real, ontology_plus_jaccard real, first_pc_cosine real, best_pc_cosine real);")
db2.commit()
#
start_time = time.time()
# domains table's attributes: table_id, column_index, domain_size, ontology_domain_size
for table_id1, column_index1, table_id2, column_index2 in \
    cursor2.execute("select distinct t1.table_id as table_id1, t1.column_index as column_id1, t2.table_id as table_id2, t2.column_index as column_index2 from (select distinct table_id, column_index from domains where ontology_domain_size>30) t1, (select distinct table_id, column_index from domains where ontology_domain_size>30) t2 where t1.table_id!=t2.table_id;").fetchall():
    if (table_id1, column_index1, table_id2, column_index2) not in seen_pairs and \
            (table_id2, column_index2, table_id1, column_index1) not in seen_pairs:
        count += 1
        if count % 100 == 0:
            print("Finished processing %d pairs." % count)
        # raw domains
        dom1 = get_domain(args.tabledir, table_id1, column_index1)
        dom2 = get_domain(args.tabledir, table_id2, column_index2)
        value_jaccard = jaccard_similarity(dom1, dom2)
        # entity domains
        entity_dom1 = get_domain(args.onttabledir, table_id1, column_index1)
        entity_dom2 = get_domain(args.onttabledir, table_id2, column_index2)
        entity_jaccard = jaccard_similarity(entity_dom1, entity_dom2)
        # class ontology domains - one level up
        annotation_dom1 = annotate_domain(entity_dom1)
        annotation_dom2 = annotate_domain(entity_dom2)
        ontology_jaccard = jaccard_similarity(annotation_dom1, annotation_dom2)
        # class ontology domains - two levels up
        annotation_dom1p = annotate_domain_plus(entity_dom1)
        annotation_dom2p = annotate_domain_plus(entity_dom2)
        ontology_plus_jaccard = jaccard_similarity(annotation_dom1p, annotation_dom2p)
        # persist class annotations
        for anno in annotation_dom1:
            cursor2.execute("insert into annotations values (?,?,?);", (table_id1, column_index1, anno))
        for anno in annotation_dom2:
            cursor2.execute("insert into annotations values (?,?,?);", (table_id2, column_index2, anno))
        for anno in annotation_dom1p:
            cursor2.execute("insert into annotations_plus values (?,?,?);", (table_id1, column_index1, anno))
        for anno in annotation_dom2p:
            cursor2.execute("insert into annotations_plus values (?,?,?);", (table_id2, column_index2, anno))
        # compute first and best pc cosine
        first_pc_cosine = None
        best_pc_cosine = -2.0
        for bin_vec1, pc_index1 in \
            cursor.execute("select pc_vec, pc_index from search_index where table_id=? and column_index=?", (table_id1, column_index1)).fetchall():
            vec1 = np.frombuffer(bin_vec1, dtype=dt)
            for bin_vec2, pc_index2 in \
                cursor.execute("select pc_vec, pc_index from search_index where table_id=? and column_index=?", (table_id2, column_index2)).fetchall():
                vec2 = np.frombuffer(bin_vec2, dtype=dt)
                cos = 1.0 - cosine(vec1, vec2)
                if pc_index2 == 0 and pc_index1 == 0:
                    first_pc_cosine = cos
                if cos > best_pc_cosine:
                    best_pc_cosine = cos
        if best_pc_cosine == -2.0:
            best_pc_cosine = None
        # persist scores
        cursor2.execute("insert into scores values (?,?,?,?,?,?,?,?,?,?);", (table_id1, column_index1, table_id2, column_index2, value_jaccard, entity_jaccard, ontology_jaccard, ontology_plus_jaccard, first_pc_cosine, best_pc_cosine))
        db2.commit()
        #
        pairs.append((table_id1, column_index1, table_id2, column_index2))
        value_jaccards.append(value_jaccard)
        entity_jaccards.append(entity_jaccard)
        ontology_jaccards.append(ontology_jaccard)
        ontology_plus_jaccards.append(ontology_plus_jaccard)
        best_pc_cosines.append(best_pc_cosine)
        first_pc_cosines.append(first_pc_cosine)
        #
        seen_pairs.append((table_id1, column_index1, table_id2, column_index2))

print("--- Execution time: %s seconds ---" % (time.time() - start_time))
print("Number of processed pairs is %d." % count)
# writing points to json
print("Saving pairs and jaccard scores...")
with open('testdata/value_jaccards.json', 'w') as fp:
    json.dump(list(value_jaccards), fp)
with open('testdata/entity_jaccards.json', 'w') as fp:
    json.dump(list(entity_jaccards), fp)
with open('testdata/ontology_jaccards.json', 'w') as fp:
    json.dump(list(ontology_jaccards), fp) 
with open('testdata/ontology_plus_jaccards.json', 'w') as fp:
    json.dump(list(ontology_plus_jaccards), fp)
with open('testdata/best_pc_cosines.json', 'w') as fp:
    json.dump(best_pc_cosines, fp)
with open('testdata/first_pc_cosines.json', 'w') as fp:
    json.dump(first_pc_cosines, fp)
with open('testdata/allpairs.json', 'w') as fp:
    json.dump(pairs, fp)
print("Done.")
