#!/usr/bin/env python3

import math
import struct

import numpy as np

from cc_pathlib import Path

ctype_map = {
	'N1' : "B",
	'Z4' : "i",
	'N4' : "I",
	'R4' : "f"
}

ntype_map = {
	'N1' : 'uint8',
	'Z4' : 'int32',
	'N4' : 'uint32',
	'R4' : 'float32',
}

sizeof_map = {
	'N1' : 1,
	'Z4' : 4,
	'N4' : 4,
	'R4' : 4,
}

class StructArray() :
	def __init__(self, meta, data) :
		self.load_meta(meta)
		self.load_data(data)

	def load_meta(self, pth) :
		self.meta = dict()
		self.var_lst = list()
		obj = pth.load()
		self.info_name, self.block_size = obj[0][0], int(obj[0][2])
		for name, ctype, offset in obj[1:] :
			if ctype == 'P4' :
				continue
			self.meta[name] = (ctype, offset)
			self.var_lst.append(name)

	def load_data(self, pth) :
		self.data = pth.read_bytes()

	def __getitem__(self, name) :
		ctype, offset = self.meta[name]
		arr = np.frombuffer(self.data, dtype=ntype_map[ctype])
		width = len(self.data) // self.block_size
		height = len(self.data) // ( width * sizeof_map[ctype] )
		arr.shape = (width, height)
		return arr[:,int(offset) // sizeof_map[ctype]]

	def extract_all(self) :
		extract_map = dict()
		for k in self.meta :
			extract_map[k] = self[k]
		return extract_map

	def get_stack(self) :
		extract_map = self.extract_all()
		data_lst = [extract_map[k] for k in self.var_lst]
		stack = [ self.var_lst, ] + [line for line in zip(* data_lst)]
		return stack

	def to_tsv(self, pth) :
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
