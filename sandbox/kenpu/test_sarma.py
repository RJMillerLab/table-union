import sys
import d
from pprint import pprint

R1, C1 = sys.argv[1:3]
R2, C2 = sys.argv[3:5]

C1 = int(C1)
C2 = int(C2)

db = d.get_db()

dom1 = d.get_domain(R1, C1)
dom2 = d.get_domain(R2, C2)

dotprod = d.jaccard_sim(db, dom1, dom2)

print("%.3f" % dotprod)
