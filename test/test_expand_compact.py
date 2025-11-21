#!/usr/bin/env python3

import sys
from cc_pathlib import Path

import structarray.rebin.meta


u = structarray.rebin.meta.MetaRebin().load(Path(sys.argv[1]))
print(u._m)
st_lst = list()
nd_lst = list()
rd_lst = list()

compact = u._proc_name_compact()
next(compact)
expand = u._proc_name_expand()
next(expand)

for st, value in u :
	nd = compact.send(st)
	rd = expand.send(nd)

	print(st, f"\x1b[34m{nd}\x1b[0m", rd)
	assert(st == rd)

