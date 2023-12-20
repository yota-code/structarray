#!/usr/bin/env python3

import hashlib
import io
import math
import os
import re
import struct

import numpy as np

from cc_pathlib import Path

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

sizeof_map = { # size of types
	'N1' : 1,
	'Z1' : 1,
	'N2' : 2,
	'Z2' : 2,
	'N4' : 4,
	'Z4' : 4,
	'R4' : 4,
	'N8' : 8,
	'Z8' : 8,
	'R8' : 8,
	'P4' : 4,
	'P8' : 8,
}

def glob_to_regex(s) :
	s = re.escape(s)
	s = s.replace('\\*', '.*?')
	return '(' + s + ')'

try :
	import h5py

	class StructHDF5() :

		h5py_opt = {
			'compression' : "gzip",
			'compression_opts' : 9,
			'shuffle' : True,
			'fletcher32' : True,
		}

		archive = False

		def __init__(self, cache_pth, interval=Ellipsis) :

			self.hdf_pth = cache_pth.resolve()
			# self.hsh_pth = self.hdf_pth.with_suffix(".tsv")

			self.key_map = dict() # key -> hsh
			self.hsh_map = dict() # hsh -> key

			self.interval = interval

			if self.hdf_pth.is_file() :
				with h5py.File(self.hdf_pth, 'r', libver="latest") as obj :
					self.key_map = {
						key : obj.attrs[key] for key in obj.attrs.keys()
					}
					self.hsh_map = {
						obj.attrs[key] : key for key in obj.attrs.keys()
					}

		def __getitem__(self, key) :
			with h5py.File(self.hdf_pth, 'r', libver="latest") as obj :
				return obj[self.key_map[key]][self.interval]

		def __setitem__(self, key, value) :
			hsh = hashlib.blake2b(value.tobytes(), digest_size=32).hexdigest()

			with h5py.File(self.hdf_pth, 'a', libver="latest") as obj :
				if hsh not in self.hsh_map :
					# print(f"+ {hsh[:9]} {('...' + key[-48:]) if 45 < len(key) else key}")
					obj.create_dataset('/' + hsh, data=value, ** self.h5py_opt)
					#print(obj.keys())
					if not self.is_archive :
						obj.attrs[key] = hsh

			self.key_map[key] = hsh
			self.hsh_map[hsh] = key
				
		def __contains__(self, key) :
			return key in self.key_map

except :
	pass

class StructArray() :

	def __init__(self, meta_pth=None, data_pth=None) :

		self.meta = None
		self.data = None
		self.cache = None

		print(f"StructArray({meta_pth}, {data_pth})")

		self.array_len = 0

		if meta_pth is not None :
			meta_pth = meta_pth.resolve()
			if meta_pth.is_file() :
				self.load_meta(meta_pth)
			else :
				raise FileNotFoundError(f"{meta_pth} does not exists")

		if data_pth is not None :
			data_pth = data_pth.resolve()
			if data_pth.is_file() :
				self.load_data(data_pth)
			else :
				raise FileNotFoundError(f"{data_pth} does not exists")

		self.extract_map = dict()
		self.extract_lst = list()

	def search(self, pattern, mode='blob') :
		# print(f"StructArray.search({pattern}, {mode})")
		if mode == 'blob':
			pattern = pattern.replace('.', '\\.').replace('*', '.*')
		elif mode == 'regexp' :
			pass
		rec = re.compile(pattern, re.IGNORECASE | re.ASCII)
		return [var for var in self.var_lst if rec.search(var) is not None]

	def load_meta(self, pth) :
		print(f"StructArray.load_meta({pth})")
		self.meta_pth = pth.resolve()

		self.meta = dict()
		self.var_lst = list()

		obj = self.meta_pth.load()
		
		first_line = obj.pop(0)
		self.struct_name = first_line[0]
		self.block_size = int(first_line[1])

		addr = 0
		for line in obj :
			if len(line) == 2 :
				name, ctype, padding = * line, 0
			elif len(line) == 3 :
				name, ctype, padding = line
			else :
				raise ValueError(f"malformed line, {line}")
			if ctype in ['P4', 'P8'] :
				pass
			else :
				self.meta[name] = (ctype, addr)
				self.var_lst.append(name)
			addr += int(padding) + sizeof_map[ctype]

		# for name, ctype, offset in obj :
		# 	if ctype in ['P4', 'P8'] :
		# 		continue
		# 	self.meta[name] = (ctype, int(offset))
		# 	self.var_lst.append(name)

		print("number of columns: ", len(self.var_lst))

	def load_data(self, pth) :
		self.data_len = pth.stat().st_size
		self.array_len = self.data_len // self.block_size
		print(f"StructArray.load_data({pth}) => {self.data_len} bytes / {self.array_len} blocks of {self.block_size} bytes")

		self.end_of_file = self.array_len * self.block_size if ( self.data_len % self.block_size != 0 ) else None
		if self.end_of_file :
			print(f"possible incomplete block at the end of data, file will be truncated at {self.end_of_file}")

		if 2**33 <= self.data_len :
			self.data = pth
			self.cache = StructHDF5(pth.with_suffix('.hdf5'))
			print("huge file, cache activated", self.cache.hsh_map)
		else :
			self.data = pth.read_bytes()[:self.end_of_file]

		print(f"data shape : {self.data_len} = {self.array_len} blocks of {self.block_size} bytes")

	def __len__(self) :
		return self.array_len

	def __contains__(self, key) :
		return key in self.var_lst

	def __iter__(self) :
		for var in self.var_lst :
			yield var

	def all_aligned(self) :
		for name in self.var_lst :
			ctype, offset = self.meta[name]
			if offset % sizeof_map[ctype] != 0 :
				print(f"{name} is not aligned: size={sizeof_map[ctype]} offeset={offset}")

	def is_aligned(self, name) :
		ctype, offset = self.meta[name]
		return offset % sizeof_map[ctype] == 0

	def __getitem__(self, name) :
		ctype, offset = self.meta[name]

		width = self.data_len // self.block_size
		height = self.data_len // ( width * sizeof_map[ctype] )

		if isinstance(self.data, Path) :
			if name not in self.cache :
				v_lst = list()
				v_len = struct.calcsize(stype_map[ctype])
				p = offset
				# print(name, offset, v_len, self.block_size)
				with self.data.open('rb') as fid :
					while p <= self.data_len :
						fid.seek(p)
						v = struct.unpack(stype_map[ctype], fid.read(v_len))[0]
						v_lst.append(v)
						p += self.block_size
						# print(p, p / self.data_len, end='\r')
				v_arr = np.array(v_lst)
				self.cache[name] = v_arr
			else :
				# print("cached", name)
				v_arr = self.cache[name]
			return v_arr
		else :
			if self.is_aligned(name) :
				arr = np.frombuffer(self.data, dtype=ntype_map[ctype])
				arr.shape = (width, height)
				return arr[:,int(offset) // sizeof_map[ctype]]
			else :
				v_lst = list()
				p = offset
				for i in range(width) :
					element = struct.unpack_from(stype_map[ctype], self.data, p)[0]
					v_lst.append(element)
					p += self.block_size
				# with io.BytesIO(self.data) as fid :
				# 	while True :
				# 		buffer = fid.read(self.block_size)
				# 		if len(buffer) == 0 :
				# 			break
				# 		v_lst.append(struct.unpack_from(stype_map[ctype], self.data, offset))
				return np.array(v_lst)

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

	def to_hdf5(self, pth) :
		import h5py

		fid = h5py.File(str(pth), 'a', libver='latest')
		for var in self :
			obj.create_dataset('/' + var, data=self[var], ** self.h5py_opt)

		fid.flush()
		fid.close()

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

# if __name__ == '__main__' :

# 	import sys

# 	data = Path(sys.argv[1])
# 	meta = data.parent / "mapping.tsv"

# 	u = StructArray(meta, data)
# 	u.to_tsv(Path("debug.tsv"))
# 	u.debug_nan(4)
