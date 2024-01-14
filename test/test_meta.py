#!/usr/bin/env python3

from cc_pathlib import Path

from structarray.meta import MetaHandler

v_lst = [
	["relative_compact.tsv", True, True],
	["absolute_compact.tsv", False, True],
	["relative_expanded.tsv", True, False],
	["absolute_expanded.tsv", False, False],
]

u = MetaHandler().load("mapping.tsv")

for name, is_relative, is_compact in v_lst :
	print(name, is_relative, is_compact)

	u.dump(name, is_relative, is_compact)
	v = MetaHandler().load(name)
	
	assert(u == v)

