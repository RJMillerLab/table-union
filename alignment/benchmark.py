import alignment

#query = '/home/fnargesian/go/src/github.com/RJMillerLab/sema-embed/toy/domains/scifi-movies-2000s.csv'
query = '/home/ekzhu/WIKI_TABLE/q1/query.csv'

cand_tables = alignment.candidates(query)

for t in cand_tables:
    print(t)
    matching, nodes1, nodes2, scores = alignment.match(query, t, alignment.bipartite_graph)
    for k, v in matching.items():
        if k in nodes1:
            print("%s <-> %s : %f" %(k,v,scores[k][v]))
            print(k)
            print(alignment.sample_query(query, k))
            print(v)
            print(alignment.sample_cand(t, v))
    #matched = [k for k,v in matching.items() if k in nodes]
    #print("table %s and %s have %d k-unionability" %(query, t, len(matching)))
    print("-----------------------------------")   

