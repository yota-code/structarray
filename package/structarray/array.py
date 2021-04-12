#!/usr/bin/env python3

import math
import re
import struct

import numpy as np

from cc_pathlib import Path

ctype_map = {
	'N1' : "B",
	'Z1' : "b",
	'Z4' : "i",
	'N4' : "I",
	'R4' : "f",
}

ntype_map = { # types numpy
	'N1' : "uint8",
	'Z4' : "int32",
	'N4' : "uint32",
	'R4' : "float32",
	'R8' : "double",
	'Z1' : "int8",
	'N8' : "uint64",
}

sizeof_map = {
	'N1' : 1,
	'Z4' : 4,
	'N4' : 4,
	'R4' : 4,
	'Z1' : 1,
	'N8' : 8,
}

def glob_to_regex(s) :
	s = re.escape(s)
	s = s.replace('\\*', '.*?')
	return '(' + s + ')'

class StructArray() :

	def __init__(self, meta=None, data=None) :

		self.meta = None
		self.data = None

		if meta is not None and meta.is_file() :
			self.load_meta(meta)

		if data is not None and data.is_file() :
			self.load_data(data)

		self.extract_map = dict()
		self.extract_lst = list()

	def load_meta(self, pth) :
		self.meta = dict()
		self.var_lst = list()

		obj = pth.load()
		
		first_line = obj.pop(0)
		self.struct_name = first_line[0]
		self.block_size = int(first_line[1])
		for name, ctype, offset in obj :
			if ctype in ['P4', 'P8'] :
				continue
			self.meta[name] = (ctype, offset)
			self.var_lst.append(name)

	def load_data(self, pth) :
		self.data = pth.read_bytes()

	def __getitem__(self, name) :
		ctype, offset = self.meta[name]
		#try :
		# print(name, ctype, offset)
		arr = np.frombuffer(self.data, dtype=ntype_map[ctype])
		#except KeyError :
		#	print("RAAAh", name)
		#	raise
			# return None
		width = len(self.data) // self.block_size
		height = len(self.data) // ( width * sizeof_map[ctype] )
		arr.shape = (width, height)
		return arr[:,int(offset) // sizeof_map[ctype]]

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
		
	def extract(self) :
		for k in self.meta :
			if k not in self.extract_map :
				self.extract_map[k] = self[k]
		return self.extract_map

	def get_stack(self) :
		data_lst = [self.extract_map[k] for k in self.var_lst]
		stack = [self.var_lst,] + [line for line in zip(* data_lst)]
		return stack

	def to_tsv(self, pth) :
		if not self.extract_map :
			self.filter_all()
		self.extract()
		pth.save(self.get_stack())

	def debug_nan(self) :
		stack = self.get_stack()
		header = stack[0]
		for line in stack[1:] :
			print(f"{line[0]} ------")
			for i, item in enumerate(line) :
				if math.isnan(item) :
					print(header[i])
			if float("nan") in line :
				print(line)

if __name__ == '__main__' :

	import matplotlib.pyplot as plt

	u = StructArray(
		Path("/mnt/workbench/source/cc-autopilot-gazebo/unrecord/mapping/scademapping.tsv"),
		Path("/mnt/workbench/source/cc-autopilot-gazebo/unrecord/replay/_active_/output.reb")
	)
	u.debug_nan()
	# u.to_tsv(Path("/tmp/test.tsv"))
