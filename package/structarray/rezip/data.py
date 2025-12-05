#!/usr/bin/env python3

import collections
import json
import time

import brotli
import h5py

try :
	import hdf5plugin
except ImportError :
	pass


import numpy as np

from cc_pathlib import Path

from structarray.data import DataGeneric
import structarray.rezip.meta

from structarray.common import *

"""
.rez or rezip formats are compact binary files based on hdf5

the mapping is embedded under a compact and compressed form
"""

class DataRezip(DataGeneric) :
	def __init__(self, data_pth:Path) :
		self.data_pth = Path(data_pth).resolve(strict=True)

		self.meta = structarray.rezip.meta.MetaRezip()

		self._load()

	def __len__(self) :
		return self.meta.array_len

	def _load(self) :
		assert self.data_pth.suffix == '.rez'

		print(f"LOADING hdf5 :: {self.data_pth}")

		start_clock = time.perf_counter_ns()
		with h5py.File(self.data_pth, 'r', libver="latest") as obj :
			self.meta.load(obj.attrs['%meta%'])
		stop_clock = time.perf_counter_ns()

		self.load_time = stop_clock - start_clock

		return self

	def __getitem__(self, name) :
		m, z, b = self.meta[name]
		if z == '=' :
			return np.ones((self.meta.array_len,), dtype=ntype_map[m]) * b
		elif z == '@' :
			with h5py.File(self.data_pth, 'r', libver="latest") as obj :
				return obj[m][b,:]
		else :
			raise ValueError
