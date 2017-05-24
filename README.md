## Development Guide

Each Go executables source code (`main.go`) is located inside a subdirectory of the `cmd` directory.
The name of the executable is the name of the subdirectory.

The Go packages live inside the top-level directory.
Importing a package use the full path. E.g., `github.com/RJMillerLab/table-union/embedding`.

## Problem Definition
Given a query table and a repository, use the tables in the repository to vertically extend 
the query table by aligning columns. 

## Contributions
1. We define the domain search problem in embedding space so that we can find domains that 
are semantically similar to a query domain. We use semantic domain search to find unionable tables.  
2. We propose a novel representation for domains in embedding space, enabling us to define unionable columns. 
3. We present a learning approach for tuning the parameters of a (distributed) Cosine LSH index, at query time, 
so that the index returns high quality unionable columns for a query column. 
4. We define the table alignment problem so that given a pair of tables with unionable column pairs 
and the matching scores, we find the best alignment. 
5. We propose an efficient alignment algorithm that given a query table a set of candidate tables, quickly identifiy the best table to align with the query table. 

## Solution Outline
### 1. Locating Unionable Tables
The first step is to find candidate tables for vertical extension by searching for columns 
that are unionable with the columns in the query table. 
Each column has an embedding vector representation which is an aggregation of the embedding vectors 
of values in the domain of the column. 
Two columns are unionable if the embedding vector of their domains have high angular similarity score. 
We use a (distributed) cosine LSH index built on the embedding vectors of columns in the repository 
to search for top-K unionable columns with a query column. 

#### 1.1 The Representation of a Column in Embedding Space
Representation 1: 
Each value in the domain of a column is represented by a vector which is the average of the embedding vectors of 
the tokens in a domain value. 
A column is represented by a vector which is the average of the embedding vectors of its 
domain values. 

Representation 2: 
Each value in the domain of a column is represented by a vector which is the sum of the embedding vectors of 
the tokens in a domain value. 
We build a domain embedding matrix by stacking the embedding vectors of the values in the domain. 
Each column is represented by the top-K principal components of its domain embedding matrix. 

#### 1.2 Unionable Columns Search
We use a (distributed) cosine LSH index built on the embedding of columns in the repository 
to search for top-K unionable columns with a query column. 
In order to pick the optimal parameters for tuning and searching the cosine LSH index, 
we apply a learning approach. 
We train a regression model that given the embedding vector of a column predicts the 
appropriate cosine similarity parameters such that the returned columns by the index 
are unionable with the query. 
In WWT benchmark created by Limaye et al., columns in tables are annotated with ontology classes. 
The semantic similarity of two columns is calculated based on the distance of their class annotations 
in the ontology using the information theoretic measure proposed by Resnik or graph traversal based measures. 
We assume two columns are unionable if they are semantically similar. 
The training samples for the regression model are generated using the embedding vectors 
of the columns in WWT benchmark, the cosine similarity scores of the embedding vectors of 
column pairs and their semantic similarity scores. 

### 2. Table Alignment Problem
Given the candidate tables that have at least one unionable column from the first step, 
we need to quickly identifiy the best table to align with the query table.
The subsequent problem is: given a pair of tables with unionable column pairs and the matching
scores, find the best alignment.

## Experiments
### 1. Unionable Table Search
Benchmarks:
Wikitables dataset: ~1.6M tables.
Open Data: 325K tables.
Webtables: 160M domains 

#### 1.1 Scalability
Point query response time

Competing approaches:
Das Sarma et al. used entity sets relatedness measure to locate tables that are 
entity complement [1]. Entity sets relatedness is the aggregation of signals obtained from 
ontologies about the similarity of the entity pairs in two sets. 

#### 1.2 Effectiveness
Ground truth:
WWT benchmark by Limaye et al. 

### 2. Table Alignment
Benchmark:      
synthetically generating unionable tables from web data to evaluate recall

#### 2.1
Competing approaches: -

## Related Work
Unionable table search:      
[1] Finding Related Tables, Das Sarma et al., SIGMOD, 2012.   
[2] Towards large-scale data discovery: position paper, Fernandez et al., WebDB, 2016.     

Word embeddings:     
[3] Enabling Cognitive Intelligence Queries in Relational Databases using Low-dimensional Word Embeddings, Bordawekar and Shmueli, arxiv, 2016.   
[4] Entity Matching on Web Tables: a Table Embeddings Approach for Blocking. Gentile et al., EDBT, 2017.


