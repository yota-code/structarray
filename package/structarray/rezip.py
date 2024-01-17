#!/usr/bin/env python3

import collections
import json

import brotli
import h5py

import numpy as np

from cc_pathlib import Path

from structarray.meta import MetaRez, ntype_map

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
			self.meta.load(obj.attrs['__map__'])

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

	

