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

from structarray.meta import MetaHandler, sizeof_map

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

ntype_map = { # types numpy
	'Z1' : "int8",
	'Z2' : "int16",
	'Z4' : "int32",
	'Z8' : "int64",
	'N1' : "uint8",
	'N2' : "uint16",
	'N4' : "uint32",
	'N8' : "uint64",
	'R4' : "float32",
	'R8' : "float64",
}

def glob_to_regex(s) :
	s = re.escape(s)
	s = s.replace('\\*', '.*?')
	return '(' + s + ')'

class RebHandler() :

	def __init__(self, data_pth, meta_pth=None, cache_disabled=False) :
		self.meta = None
		self.data = None

		self.cache_disabled = cache_disabled

		self.array_len = 0

		data_pth = Path(data_pth).resolve()
		meta_pth = data_pth.parent / "mapping.tsv" if meta_pth is None else Path(meta_pth).resolve()

		self.meta = MetaHandler().load(meta_pth)
		self.data = self.load(data_pth)
		
		# the following is deprecated
		self.extract_map = dict()
		self.extract_lst = list()

	def load(self, pth) :

		self.data_len = pth.stat().st_size
		self.array_len = self.data_len // self.meta.sizeof
		print(f"LOADING {pth} => {self.data_len} bytes / {self.array_len} blocks of {self.meta.sizeof} bytes")

		self.end_of_file = self.array_len * self.meta.sizeof if ( self.data_len % self.meta.sizeof != 0 ) else None
		if self.end_of_file :
			print(f"! possible incomplete block at the end of data, file will be truncated at {self.end_of_file}")

		if 2**32 <= self.data_len :
			self.data = pth # direct file access mode for files of more than 4 GiBytes
			if not self.cache_disabled :
				try :
					from structarray.cache import CacheHandler
					self.cache = CacheHandler(pth.with_suffix('.hdf5'))
				except ModuleNotFoundError :
					self.cache = dict()
				print("! huge file detected, cache activated")
		else :
			self.data = pth.read_bytes()[:self.end_of_file]
			self.cache_disabled = True

	def __len__(self) :
		return self.array_len
	
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

		if self.is_aligned(name) :
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
				v_arr = self.parse_line_from_file(name)
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

	def to_rez(self, pth) :
		import h5py
		import brotli

		h5py_opt = {
			'compression' : "gzip",
			'compression_opts' : 9,
			'shuffle' : True,
			'fletcher32' : True,
		}

		v_lst = self.var_lst
		r_lst = [name for name, mtype, addr in self.meta._dump_addr(False, True)]

		if pth.is_file() :
			pth.unlink()

		e_map = dict()
		for c in ['R8', 'R4', 'Z4', 'Z2', 'Z1', 'N4', 'N2', 'N1'] :
			i_lst = [i for i, v in enumerate(v_lst) if self.meta[v][0] == c]
			print(c, len(i_lst))
			if i_lst :
				e_map[c] = dict() # ctype -> name -> position
				s = collections.defaultdict(set) # hash -> position set
				m = list()
				for i in i_lst :
					v = v_lst[i] # full name of the variable
					d = self[v] # full data line extracted
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

				with h5py.File(pth, 'a', libver="latest") as obj :
					w = np.vstack(m)
					print(w.shape, w.dtype)
					obj.create_dataset('/' + c, data=w, ** h5py_opt)
				print("size:", pth.stat().st_size)

		f_lst = list()
		for i, (v, r) in enumerate(zip(v_lst, r_lst)) :
			ctype = self.meta[v][0]
			f_lst.append(f'{r}\t{ctype}\t{e_map[ctype][i]}')

		with h5py.File(pth, 'a', libver="latest") as obj :
			obj.attrs['__map__'] = np.void(brotli.compress('\n'.join(f_lst).encode('ascii'))) # https://docs.h5py.org/en/stable/strings.html

# if __name__ == '__main__' :

# 	import sys

# 	data = Path(sys.argv[1])
# 	meta = data.parent / "mapping.tsv"

# 	u = StructArray(meta, data)
# 	u.to_tsv(Path("debug.tsv"))
# 	u.debug_nan(4)
