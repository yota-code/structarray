#!/usr/bin/env python3

import io
import math
import os
import re
import struct

import numpy as np

from cc_pathlib import Path

ctype_map = { # types of struct
	'Z1' : "b",
	'Z4' : "i",
	'Z8' : "q",
	'N1' : "B",
	'N4' : "I",
	'N8' : "Q",
	'R4' : "f",
	'R8' : "d",
}

ntype_map = { # types numpy
	'Z1' : "int8",
	'Z4' : "int32",
	'Z8' : "int64",
	'N1' : "uint8",
	'N4' : "uint32",
	'N8' : "uint64",
	'R4' : "float32",
	'R8' : "float64",
}

sizeof_map = { # size of types
	'N1' : 1,
	'Z4' : 4,
	'N4' : 4,
	'R4' : 4,
	'Z1' : 1,
	'N8' : 8,
	'R8' : 8,
}

def glob_to_regex(s) :
	s = re.escape(s)
	s = s.replace('\\*', '.*?')
	return '(' + s + ')'

class StructArray() :

	def __init__(self, meta=None, data=None) :

		self.meta = None
		self.data = None

		print(f"StructArray({meta}, {data})")

		self.length = 0

		if meta is not None :
			self.meta = meta.resolve()
			if self.meta.is_file() :
				self.load_meta(meta)

		if data is not None :
			self.data = data.resolve()
			if self.data.is_file() :
				self.load_data(self.data)

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
		self.meta = dict()
		self.var_lst = list()

		obj = pth.load()
		
		first_line = obj.pop(0)
		self.struct_name = first_line[0]
		self.block_size = int(first_line[1])
		for name, ctype, offset in obj :
			if ctype in ['P4', 'P8'] :
				continue
			self.meta[name] = (ctype, int(offset))
			self.var_lst.append(name)

		print(len(self.var_lst))

	def load_data(self, pth) :
		self.data = pth.read_bytes()
		self.length = len(self.data) // self.block_size
		if len(self.data) % self.block_size != 0 :
			self.data = self.data[:self.length * self.block_size]
			print(f"incomplete block at the end of data, truncated at {len(self.data)}")
		print(f"data shape : {len(self.data)} = {self.length} blocks of {self.block_size} bytes")

	def __len__(self) :
		return self.length

	def __contains__(self, key) :
		return key in self.var_lst

	def __iter__(self) :
		for var in self.var_lst :
			yield var

	def __getitem__(self, name) :
		ctype, offset = self.meta[name]

		width = len(self.data) // self.block_size
		height = len(self.data) // ( width * sizeof_map[ctype] )
		
		if self.block_size % sizeof_map[ctype] == 0 and offset % sizeof_map[ctype] == 0 :
			arr = np.frombuffer(self.data, dtype=ntype_map[ctype])
			arr.shape = (width, height)
			return arr[:,int(offset) // sizeof_map[ctype]]
		else :
			print("RAAAAH")
			v_lst = list()
			p = offset
			for i in range(width) :
				element = struct.unpack_from(ctype_map[ctype], self.data, p)[0]
				print(element)
				v_lst.append(element)
				p += self.block_size
			# with io.BytesIO(self.data) as fid :
			# 	while True :
			# 		buffer = fid.read(self.block_size)
			# 		if len(buffer) == 0 :
			# 			break
			# 		v_lst.append(struct.unpack_from(ctype_map[ctype], self.data, offset))
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
