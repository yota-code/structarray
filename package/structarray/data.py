#!/usr/bin/env python3

import collections

from abc import ABC

class DataGeneric(ABC) :

	def debug_unfinite(self) :
		r_map = {"nan": collections.defaultdict(set), "inf": collections.defaultdict(set)}
		for name, (mtype, addr) in self.meta._m.items() :
			if mtype[0] != "R" :
				continue
			y = self[name]
			for k, f in [('nan', np.isnan), ('inf', np.isinf)] :
				u = f(y)
				p = u.nonzero()[0]
				if 0 < p.size :
					r_map[k][int(p[0])].add(name)

		for k in r_map :
			s_lst = list()
			for p in sorted(r_map[k]) :
				s_lst.append(f'@{p}')
				for name in sorted(r_map[k][p]) :
					s_lst.append(f'\t{name}')
			if s_lst :
				self.data_pth.with_suffix(f'.debug_{k}.tsv').write_text('\n'.join(s_lst))
