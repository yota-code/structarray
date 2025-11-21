#!/usr/bin/env python3

import sys

import numpy as np
import matplotlib.pyplot as plt

from cc_pathlib import Path

from structarray.rebin import DataRebin
from structarray.rezip import DataRezip

reb_pth = Path(sys.argv[1]).resolve()
reb = DataRebin(reb_pth)
rez_pth = reb_pth.with_suffix('.rez')
rez = DataRezip(rez_pth)

for k, v in reb.meta :
	if v[0].startswith('P') :
		continue
	a = reb[k]
	b = rez[k]
	if not ((a == b) | (np.isnan(a) & np.isnan(b))).all() :
		print(reb.meta[k])
		print(a, a.dtype)
		print(rez.meta[k])
		print(b, b.dtype)
		print(k)
		plt.plot(a)
		plt.plot(b)
		plt.show()
		break