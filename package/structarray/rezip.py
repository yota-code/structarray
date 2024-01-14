#!/usr/bin/env python3

import collections
import json

import brotli
import h5py

import numpy as np

from cc_pathlib import Path

"""
.rez or rezip formats are compact binary files based on hdf5

the mapping is embedded under a compact and compressed form
"""

from structarray.array import StructArray as RebHandler

class RezHandler() :
	def __init__(self, pth) :
		self.pth = pth.resolve()

		# if self.pth.is_file() :
		# 	self.load()

	def load(self) :
		with h5py.File(self.pth, 'r', libver="latest") as obj :
			var_zip = obj.attrs['__map__']
			var_bin = brotli.decompress(var_zip)
			var_txt = var_bin.decode('ascii')
			var_map = json.loads(var_txt)
			self.v_lst = self.expand_name_lst()

	def compact_name_lst(self, v_lst) :
		# validated
		# remove duplicate parts from the variable names
		r_lst = list()
		p_lst = list()
		for v in v_lst :
			n_lst = v.split('.')
			q = 0
			for p, n in zip(p_lst, n_lst) :
				if p != n :
					break
				q += 1
			r_lst.append((f"{q}/" if q else '') + '.'.join(n_lst[q:]))
			p_lst = n_lst
		return r_lst

	def expand_name_lst(self, r_lst) :
		# validated
		# undo the compaction and give back the original names
		v_lst = list()
		p_lst = list()
		for r in r_lst :
			if '/' in r :
				c, sep, z = r.partition('/')
				n_lst = p_lst[:int(c)] + z.split('.')
				v_lst.append('.'.join(n_lst))
			else :
				v_lst.append(r)
				n_lst = r.split('.')
			p_lst = n_lst
		return v_lst
	
	def convert_from_reb(self, data_pth, meta_pth=None) :

		h5py_opt = {
			'compression' : "gzip",
			'compression_opts' : 9,
			'shuffle' : True,
			'fletcher32' : True,
		}

		data_pth = data_pth.resolve()
		if meta_pth is None :
			meta_pth = data_pth.parent / "mapping.tsv"

		reb = RebHandler(meta_pth, data_pth)

		v_lst = reb.var_lst
		r_lst = self.compact_name_lst(v_lst)

		if self.pth.is_file() :
			self.pth.unlink()

		e_map = dict()
		for c in ['R8', 'R4', 'Z4', 'Z2', 'Z1', 'N4', 'N2', 'N1'] :
			i_lst = [i for i, v in enumerate(v_lst) if reb.meta[v][0] == c]
			print(c, len(i_lst))
			if i_lst :
				e_map[c] = dict() # ctype -> name -> position
				s = collections.defaultdict(set) # hash -> position set
				m = list()
				for i in i_lst :
					v = v_lst[i] # full name of the variable
					d = reb[v] # full data line extracted
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
					e_map[c][i] = j
					print(f"  {i} {j}\x1b[k", end='\r')

				Path(f"s_map.{c}.json").save(s)

				with h5py.File(self.pth, 'a', libver="latest") as obj :
					w = np.vstack(m)
					print(w.shape, w.dtype)
					obj.create_dataset('/' + c, data=w, ** h5py_opt)
				print("size:", self.pth.stat().st_size)

		f_lst = list()
		for i, (v, r) in enumerate(zip(v_lst, r_lst)) :
			ctype = reb.meta[v][0]
			f_lst.append(f'{r}\t{ctype}\t{e_map[ctype][i]}')

		with h5py.File(self.pth, 'a', libver="latest") as obj :
			obj.attrs['__map__'] = np.void(brotli.compress('\n'.join(f_lst).encode('ascii'))) # https://docs.h5py.org/en/stable/strings.html
