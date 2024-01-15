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
	
