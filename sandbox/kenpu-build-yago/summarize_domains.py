import sys
import d
from pprint import pprint

R1, C1 = sys.argv[1:3]
R2, C2 = sys.argv[3:5]

C1 = int(C1)
C2 = int(C2)

db = d.get_db()

dom1 = d.get_domain(R1, C1)
h1 = d.get_cat2(db, dom1) + d.get_types(db, dom1)
ent1 = d.get_entropy(h1)
nent1 = d.get_normalized_entropy(h1)

dom2 = d.get_domain(R2, C2)
h2 = d.get_cat2(db, dom2) + d.get_types(db, dom2)
ent2 = d.get_entropy(h2)
nent2 = d.get_normalized_entropy(h2)

delta_ent = d.entropy_change(h1, h2)
delta_ent2 = d.entropy_change(h2, h1)
delta_nent = d.normalized_entropy_change(h1, h2)
delta_nent2 = d.normalized_entropy_change(h2, h1)

# print("ENTROPY: %.3f   %.3f    %.3f / %.3f" % (ent1, ent2, delta_ent, delta_ent2))
print("N-ENTRO: %.3f   %.3f    %.3f / %.3f" % (nent1, nent2, delta_nent, delta_nent2))
