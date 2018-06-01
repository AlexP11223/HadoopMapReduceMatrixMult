# https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.random.html
from sys import argv
from scipy.sparse import random
from scipy import stats
import numpy as np


class CustomRandomState(object):
    def randint(self, k):
        i = np.random.randint(k)
        return i - i % 2


if len(argv) < 3:
    print('Usage: row_cound column_count [id]')
    print('Example: 400 500 N')
    exit()

row_count = int(argv[1])
column_count = int(argv[2])
id = ''
if len(argv) > 3:
    id = argv[3]

rs = CustomRandomState()
rvs = stats.poisson(25, loc=10).rvs
matr = random(row_count, column_count, density=0.25, random_state=rs, data_rvs=rvs).tocsr()

nonzero_indexes = matr.nonzero()

s = ''
for i, r in enumerate(nonzero_indexes[0]):
    c = nonzero_indexes[1][i]
    s += ','.join(filter(None, [id, str(r), str(c), str(int(matr[r, c]))])) + '\n'

with open('_'.join(filter(None, [id.lower(), str(row_count), str(column_count)])) + '.txt', 'w') as file:
    file.write(s)
