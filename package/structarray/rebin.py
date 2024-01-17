#!/usr/bin/env python3

import collections
import hashlib
import io
import math
import os
import re
import struct

import numpy as np

from cc_pathlib import Path

from structarray.meta import MetaReb, sizeof_map, ntype_map, compact_name

stype_map = { # types of struct
	'Z1' : "b",
	'Z2' : "h",
	'Z4' : "i",
	'Z8' : "q",
	'N1' : "B",
	'N2' : "H",
	'N4' : "I",
	'N8' : "Q",
	'R4' : "f",
	'R8' : "d",
}

def glob_to_regex(s) :
	s = re.escape(s)
	s = s.replace('\\*', '.*?')
	return '(' + s + ')'

class RebHandler() :

	def __init__(self, cache_disabled=False) :
		self.meta = MetaReb()
		self.data = None

		self.cache_disabled = cache_disabled

		self.array_len = 0
		
		# the following is deprecated
		self.extract_map = dict()
		self.extract_lst = list()

	def __len__(self) :
		return self.array_len
	
	def load(self, data_pth, meta_pth=None) :

		self.data_pth = Path(data_pth).resolve()
		assert self.data_pth.suffix == '.reb'

		self.meta_pth = self.data_pth.parent / "mapping.tsv" if meta_pth is None else Path(meta_pth).resolve()
		self.meta.load(self.meta_pth)

		self.data_len = self.data_pth.stat().st_size
		self.array_len = self.data_len // self.meta.sizeof
		print(f"LOADING...\ndata: {self.data_pth}\nmeta:{self.meta_pth}\n => {self.data_len} bytes or {self.array_len} blocks of {self.meta.sizeof} bytes\n")

		self.end_of_file = self.array_len * self.meta.sizeof if ( self.data_len % self.meta.sizeof != 0 ) else None
		if self.end_of_file :
			print(f"! possible incomplete block at the end of data, file will be truncated at {self.end_of_file}")

		if 2**32 <= self.data_len :
			self.data = self.data_pth # direct file access mode for files of more than 4 GiBytes
			if not self.cache_disabled :
				try :
					from structarray.cache import CacheHandler
					self.cache = CacheHandler(self.data_pth.with_suffix('.__cache__.hdf5'))
				except ModuleNotFoundError :
					self.cache = dict()
				print("! huge file detected, cache activated")
		else :
			self.data = self.data_pth.read_bytes()[:self.end_of_file]
			self.cache_disabled = True

		return self

	def get_from_file(self, name) :
		ctype, offset = self.meta[name]

		v_len = struct.calcsize(stype_map[ctype])
		v_lst = list()
		pos = offset
		with self.data.open('rb') as fid :
			while pos <= self.data_len :
				fid.seek(pos)
				v = struct.unpack(stype_map[ctype], fid.read(v_len))[0]
				v_lst.append(v)
				pos += self.meta.sizeof

		return np.array(v_lst)
	
	def get_from_buffer(self, name) :
		ctype, offset = self.meta[name]
		width = self.data_len // self.meta.sizeof
		height = self.data_len // ( width * sizeof_map[ctype] )

		if self.meta.is_aligned(name) :
			arr = np.frombuffer(self.data, dtype=ntype_map[ctype])
			arr.shape = (width, height)
			return arr[:,int(offset) // sizeof_map[ctype]]
		else :
			v_lst = list()
			pos = offset
			for i in range(width) :
				v = struct.unpack_from(stype_map[ctype], self.data, pos)[0]
				v_lst.append(v)
				pos += self.meta.sizeof
			return np.array(v_lst)

	def __getitem__(self, name) :
		if isinstance(self.data, Path) :
			if self.cache_disabled or name not in self.cache :
				v_arr = self.get_from_file(name)
				if not self.cache_disabled :
					self.cache[name] = v_arr
			else :
				v_arr = self.cache[name]
			return v_arr
		else :
			return self.get_from_buffer(name)

	def filter_select(self, * filter_lst) :
		filter_lst = [glob_to_regex(line) for line in filter_lst]
		filter_rec = re.compile('|'.join(filter_lst))

		for var in self.var_lst :
			if filter_rec.search(var) :
				self.extract_lst.append(var)

	def filter_all(self) :
		self.extract_lst = self.var_lst[:]

	def filter_reset(self) :
		self.extract_lst = list()
		
	def extract(self, start=None, stop=None) :
		for k in self.meta :
			if k not in self.extract_map :
				self.extract_map[k] = self[k][start:stop]
		return self.extract_map

	def get_stack(self) :
		data_lst = [self.extract_map[k] for k in self.var_lst]
		stack = [self.var_lst,] + [line for line in zip(* data_lst)]
		return stack

	def to_tsv(self, pth, start=None, stop=None) :
		if not self.extract_map :
			self.filter_all()
		self.extract(start, stop)
		pth.save(self.get_stack())

	def to_listing(self, pth, at=None) :
		if not self.extract_lst :
			self.filter_all()

		if 'STRUCTARRAY_listing_SLICE' in os.environ :
			s = slice(* [int(i) for i in os.environ['STRUCTARRAY_listing_SLICE'].split(':')])
		elif at is not None :
			s = slice(at, at + 1)
		else :
			s = slice(0, 10)

		stack = [[k,] + list(self[k][s]) for k in self.extract_lst]
		pth.save(stack)

	def debug(self, pth) :
		self.extract()
		stack = self.get_stack()
		header = stack[0]
		has_error = False
		print("\x1b[31mNan\x1b[0m")
		print("\x1b[32mInf\x1b[0m")
		for n, line in enumerate(stack[1:]) :
			print(f"---  {n}")
			for i, item in enumerate(line) :
				if header[i] in [
					'_C_3_upmv_core._C_1_C__root__._C_1_C_goaround._C_1_C_goaround_ver._L115_upmv_app'
				] :
					continue
				if math.isnan(item) :
					print(f"NAN \x1b[31m{header[i]}\x1b[0m")
					has_error = True
				# if math.isinf(item) :
				# 	print(f"INF \x1b[32m{header[i]}\x1b[0m")
				# 	has_error = True
			if has_error :
				break

		self.to_listing(pth, n)
		self.to_listing(pth.with_suffix('.1.tsv'), n-1)

	def to_rez(self) :
		import h5py
		import brotli

		archive_pth = self.data_pth.with_suffix('.rez')

		h5py_opt = {
			'compression' : "gzip",
			'compression_opts' : 9,
			'shuffle' : True,
			'fletcher32' : True,
		}

		v_lst = list(self.meta)
		r_lst = compact_name(v_lst)

		# assert len(v_lst) == len(r_lst)

		if archive_pth.is_file() :
			archive_pth.unlink()

		e_map = dict()
		for c in ['R8', 'R4', 'Z4', 'Z2', 'Z1', 'N4', 'N2', 'N1'] :
			i_lst = [i for i, v in enumerate(v_lst) if self.meta[v][0] == c]
			print(c, len(i_lst))
			if i_lst :
				e_map[c] = dict() # ctype -> name -> position
				s = collections.defaultdict(set) # hash -> position set
				m = list()
				for n, i in enumerate(i_lst) :
					v = v_lst[i] # full name of the variable
					d = self[v] # full data line extracted
					if d[0] == d[-1] and (d[0] == d).all() :
						e_map[c][i] = ('=', d[0])
						print(f"\x1b[A\x1b[K{n+1:7d} / {len(i_lst)} # {v_lst[i]} = {d[0]}")
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
						print(f"\x1b[A\x1b[K{n+1:7d} / {len(i_lst)} # {v_lst[i]} @ {j}")

				Path(f"s_map.{c}.json").save(s)

				with h5py.File(archive_pth, 'a', libver="latest") as obj :
					w = np.vstack(m)
					print(c, w.shape, w.dtype)
					obj.create_dataset('/' + c, data=w, ** h5py_opt)
				# print("size:", archive_pth.stat().st_size)

		f_lst = [str(self.array_len),] # on doit garder array_len dans les méta données parce qu'il se peut que TOUS les vecteurs soient constants
		for i, (v, r) in enumerate(zip(v_lst, r_lst)) :
			mtype = self.meta[v][0]
			z, j = e_map[mtype][i]
			f_lst.append(f'{r}\t{mtype}{z}{j}')

		Path(archive_pth.with_suffix('.tsv')).write_text('\n'.join(f_lst))

		with h5py.File(archive_pth, 'a', libver="latest") as obj :
			obj.attrs['__map__'] = np.void(brotli.compress('\n'.join(f_lst).encode('ascii'), mode=brotli.MODE_TEXT)) # https://docs.h5py.org/en/stable/strings.html

		data_size = self.data_pth.stat().st_size
		meta_size = self.meta_pth.stat().st_size
		archive_size = archive_pth.stat().st_size
		print(f"original: {data_size + meta_size:15d} bytes ({data_size} data + {meta_size} meta)\n archive: {archive_size:15d} bytes\n => archive takes {100.0 * archive_size / (data_size + meta_size):0.5}% of original")

		return archive_pth