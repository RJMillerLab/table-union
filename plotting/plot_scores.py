import argparse, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import json


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--dataset", default="/home/fnargesian/WIKI_TABLE/search-index.db")
parser.add_argument("-oa", "--outputap", default="plots/best_cosine_ontology_1level_jaccard.pdf")
parser.add_argument("-ob", "--outputbp", default="plots/first_cosine_ontology_1level_jaccards.pdf")
parser.add_argument("-oap", "--outputa", default="plots/best_cosine_ontology_2level_jaccard.pdf")
parser.add_argument("-obp", "--outputb", default="plots/first_cosine_ontology_2level_jaccards.pdf")
parser.add_argument("-oc", "--outputc", default="plots/best_cosine_entity_jaccard.pdf")
parser.add_argument("-od", "--outputd", default="plots/first_cosine_entity_jaccard.pdf")
parser.add_argument("-oe", "--outpute", default="plots/best_cosine_value_jaccard.pdf")
parser.add_argument("-of", "--outputf", default="plots/first_cosine_value_jaccard.pdf")
args = parser.parse_args(sys.argv[1:])

pairs = []
best_pc_cosines = []
first_pc_cosines = []
value_jaccards = []
entity_jaccards = []
ontology_jaccards = []
# loading domain pairs and jaccard scores
with open('testdata/value_jaccards.json', 'r') as fp:
    value_jaccards = json.load(fp)
with open('testdata/entity_jaccards.json', 'r') as fp:
    entity_jaccards = json.load(fp)
with open('testdata/ontology_jaccards.json', 'r') as fp:
    ontology_jaccards = json.load(fp)
with open('testdata/ontology_plus_jaccards.json', 'r') as fp:
    ontology_plus_jaccards = json.load(fp)
with open('testdata/best_pc_cosines.json', 'r') as fp:
    best_pc_cosines = json.load(fp)
with open('testdata/first_pc_cosines.json', 'r') as fp:
    first_pc_cosines = json.load(fp)
with open('testdata/allpairs.json', 'r') as fp:
    pairs = json.load(fp)
# plotting ontology jaccard vs best pc cosine
print("Plotting...")
plt.figure(figsize=(18, 18))
plt.xlabel('best pc cosine', fontsize=24)
plt.ylabel('ontology jaccard', fontsize=24)
plt.title('best pc cosine vs ontology (two levels of abstraction) jaccard of column pairs', fontsize=24)
plt.scatter(best_pc_cosines, ontology_plus_jaccards)
plt.savefig(args.outputa)
# plotting ontology jaccard vs first pc cosine
plt.figure(figsize=(18, 18))
plt.xlabel('first pc cosine', fontsize=24)
plt.ylabel('ontology jaccard', fontsize=24)
plt.title('first pc cosine vs ontology (two levels of abstraction) jaccard of column pairs', fontsize=24)
plt.scatter(first_pc_cosines, ontology_plus_jaccards)
plt.savefig(args.outputb)
# plotting ontology jaccard vs first pc cosine
plt.figure(figsize=(18, 18))
plt.xlabel('best pc cosine', fontsize=24)
plt.ylabel('ontology jaccard', fontsize=24)
plt.title('best pc cosine vs ontology (one level of abstraction) jaccard of column pairs', fontsize=24)
plt.scatter(best_pc_cosines, ontology_jaccards)
plt.savefig(args.outputap)
# plotting ontology jaccard vs first pc cosine
plt.figure(figsize=(18, 18))
plt.xlabel('first pc cosine', fontsize=24)
plt.ylabel('ontology jaccard', fontsize=24)
plt.title('first pc cosine vs ontology (one level of abstraction) jaccard of column pairs', fontsize=24)
plt.scatter(first_pc_cosines, ontology_jaccards)
plt.savefig(args.outputbp)
# best pc cosine vs entity jaccard
plt.figure(figsize=(18, 18))
plt.xlabel('best pc cosine', fontsize=24)
plt.ylabel('entity jaccard', fontsize=24)
plt.title('best pc cosine vs entity jaccard of column pairs', fontsize=24)
plt.scatter(best_pc_cosines, entity_jaccards)
plt.savefig(args.outputc)
# first pc cosine vs entity jaccard
plt.figure(figsize=(18, 18))
plt.xlabel('first pc cosine', fontsize=24)
plt.ylabel('entity jaccard', fontsize=24)
plt.title('first pc cosine vs entity jaccard of column pairs', fontsize=24)
plt.scatter(first_pc_cosines, entity_jaccards)
plt.savefig(args.outputd)
# best pc cosine vs value jaccard
plt.figure(figsize=(18, 18))
plt.xlabel('best pc cosine', fontsize=24)
plt.ylabel('entity jaccard', fontsize=24)
plt.title('best pc cosine vs entity jaccard of column pairs', fontsize=24)
plt.scatter(best_pc_cosines, value_jaccards)
plt.savefig(args.outpute)
# first pc cosine vs value jaccard
plt.figure(figsize=(18, 18))
plt.xlabel('first pc cosine', fontsize=24)
plt.ylabel('entity jaccard', fontsize=24)
plt.title('first pc cosine vs entity jaccard of column pairs', fontsize=24)
plt.scatter(first_pc_cosines, value_jaccards)
plt.savefig(args.outputf)
print("Done.")
