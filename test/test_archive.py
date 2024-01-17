#!/usr/bin/env python3

import sys

import numpy as np
import matplotlib.pyplot as plt

from cc_pathlib import Path

from structarray.rebin import RebHandler
from structarray.rezip import RezHandler

reb_pth = Path(sys.argv[1]).resolve()
reb = RebHandler().load(reb_pth)
rez_pth = reb.to_rez()
rez = RezHandler().load(rez_pth)

for name in reb.meta :
	a = reb[name]
	b = rez[name]
	if not ((a == b) | (np.isnan(a) & np.isnan(b))).all() :
		print(reb.meta[name])
		print(a, a.dtype)
		print(rez.meta[name])
		print(b, b.dtype)
		print(name)
		plt.plot(a)
		plt.plot(b)
		plt.show()
		break