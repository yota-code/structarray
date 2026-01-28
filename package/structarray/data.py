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

	def to_hdf5(self) :
		""" for compatibility with matlab, let's keep it simple """

		import h5py

		h5py_opt = {
			'compression' : "gzip",
			'compression_opts' : 9,
			'shuffle' : True,
		}

		archive_pth = self.data_pth.with_suffix('.hdf5')
		archive_pth.unlink(missing_ok=True)

		with h5py.File(archive_pth, 'a', libver="latest") as obj :
			for i, (name, data) in enumerate(self) :
				print(f"\x1b[A\x1b[K{int(round(100.0 * i / len(self.meta))):3d}% {name}", flush=True)
				obj.create_dataset(f'/{name}', data=data, ** h5py_opt)
