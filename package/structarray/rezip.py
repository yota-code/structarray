#!/usr/bin/env python3

import collections
import json

import brotli
import h5py

try :
	import hdf5plugin
except ImportError :
	pass


import numpy as np

from cc_pathlib import Path

import structarray
from structarray.meta import ntype_map, compact_name, expand_name

"""
.rez or rezip formats are compact binary files based on hdf5

the mapping is embedded under a compact and compressed form
"""

class RezHandler() :
	def __init__(self) :
		self.meta = MetaRez()

	def load(self, pth) :
		self.pth = Path(pth).resolve()

		assert self.pth.suffix == '.rez'

		with h5py.File(self.pth, 'r', libver="latest") as obj :
			self.meta.load(obj.attrs['_meta'])

		return self

	def __getitem__(self, name) :
		m, z, b = self.meta[name]
		if z == '=' :
			return np.ones((self.meta.array_len,), dtype=ntype_map[m]) * b
		elif z == '@' :
			with h5py.File(self.pth, 'r', libver="latest") as obj :
				return obj[m][b,:]
		else :
			raise ValueError


def reb_to_rez(data, meta=None) :
	""" en deux passes ? la première repère les vecteurs constants ou identiques 
	la deuxième fourre tout dans un hdf5 ? mais ça fait lire le fichier 2 fois
	
	ou alors on stocke dans des fichiers temporaires pour chaque type

	"""

	p = "\x1b[A\x1b[K"

	match data :
		case Path() | str() :
			data = structarray.data.DataHandler(Path(data).resolve(strict=True), meta)
		case structarray.data.DataHandler() :
			data = data

	meta = data.meta

	import brotli
	import h5py

	archive_pth = data.data_pth.with_suffix('.rez')

	h5py_opt = {
		'compression' : "gzip",
		'compression_opts' : 9,
		'shuffle' : True,
		'fletcher32' : True,
	}

	v_lst = list(meta._m)
	r_lst = compact_name(v_lst)

	# assert len(v_lst) == len(r_lst)

	if archive_pth.is_file() :
		archive_pth.unlink()

	e_map = dict()
	for c in ['R8', 'R4', 'Z4', 'Z2', 'Z1', 'N4', 'N2', 'N1'] :
		i_lst = [i for i, v in enumerate(v_lst) if meta[v][0] == c]
		print(f"{c} {0:7d} / {len(i_lst)}")
		if i_lst :
			e_map[c] = dict() # ctype -> name -> position
			s = collections.defaultdict(set) # hash -> position set
			m = list()
			for n, i in enumerate(i_lst) :
				v = v_lst[i] # full name of the variable
				d = data[v] # full data line extracted
				if d[0] == d[-1] and (d[0] == d).all() :
					e_map[c][i] = ('=', d[0])
					print(f"{p}{c} {n+1:7d} / {len(i_lst)} # {d[0]} = {v_lst[i]}")
				else :
					h = hash(d.tobytes()) # hash of the line
					j = len(m)
					if h not in s :
						s[h].add(j)
						m.append(d)
					else :
						for j in s[h] :
							if d.tobytes() == m[j].tobytes() :
								break
						else :
							s[h].add(j)
							m.append(d)
					e_map[c][i] = ('@', j)
					print(f"{p}{c} {n+1:7d} / {len(i_lst)} # {j} @ {v_lst[i]}")

			Path(f"s_map.{c}.json").save(s)
			
			if m :
				with h5py.File(archive_pth, 'a', libver="latest") as obj :
					w = np.vstack(m)
					print(f"{p}{c} {len(i_lst):7d} / {len(i_lst)} => {w.shape[0]} rows")
					obj.create_dataset('/' + c, data=w, ** h5py_opt)

	f_lst = [str(data.vector_len),] # on doit garder vector_len dans les méta données parce qu'il se peut que TOUS les vecteurs soient constants
	for i, (v, r) in enumerate(zip(v_lst, r_lst)) :
		mtype = meta[v][0]
		if mtype in e_map :
			z, j = e_map[mtype][i]
			f_lst.append(f'{r}\t{mtype}{z}{j}')

	Path(archive_pth.with_suffix('.mez')).write_text('\n'.join(f_lst))

	with h5py.File(archive_pth, 'a', libver="latest") as obj :
		meta_zip = brotli.compress('\n'.join(f_lst).encode('ascii'), mode=brotli.MODE_TEXT)
		obj.attrs['_meta'] = np.void(meta_zip) # https://docs.h5py.org/en/stable/strings.html

	data_size = data.data_pth.stat().st_size
	meta_size = meta.meta_pth.stat().st_size
	archive_size = archive_pth.stat().st_size

	print(f"\noriginal: {data_size + meta_size:15d} bytes ({meta_size:8d} meta)\n archive: {archive_size:15d} bytes ({len(meta_zip):8d} meta)\n => archive takes {100.0 * archive_size / (data_size + meta_size):0.3}% of original")

	return archive_pth