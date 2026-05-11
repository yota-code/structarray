#!/usr/bin/env python3

import collections
import hashlib
import io
import math
import mmap
import os
import re
import struct
import time

import numpy as np

from cc_pathlib import Path

import structarray.rebin.meta

from structarray.data import DataGeneric
from structarray.common import *

class DataRebin(DataGeneric) :
	"""
	this class aims to handle a single .reb file.
	a .reb file is the native structarray file, it only consists in raw C structures dumped directly and successively into a file
	in order to access fields by name, a MetaReb instance must be provided
	"""

	use_cache = False
	use_mmap = True
	block_boundary = 8

	def __init__(self, data_pth:Path, meta=None) :
		"""
		data_pth must point to an existing .reb file
		meta can either be:
			- a meta_pth which point to a valid context
			- an existing MetaRebin object
			- or None, in which case, the handler try to open a context_map.tsv
			  or compact_map.tsv file in the same directory
		"""

		self.data_pth = Path(data_pth).resolve(strict=True)
		print("debug meta : ",meta)
		match meta :
			case Path() | str() :
				self.meta = structarray.rebin.meta._cached_meta[Path(meta).resolve(strict=True)]
			case structarray.rebin.meta.MetaRebin() :
				self.meta = meta
			case None :
				# si meta=None, on essaie d'ouvrir un fichier meta à côté du fichier data
				for k in ["context_map.tsv", "compact_map.tsv"] :
					pth = (self.data_pth.parent / k).resolve()
					if pth.is_file() :
						self.meta = structarray.rebin.meta._cached_meta[pth]
						break
			case _ :
				raise ValueError("meta can't be loaded")
		print("debug meta : ",self.meta)		
		self._load()
		
	def _load(self) :

		assert self.data_pth.suffix == '.reb'

		# taille du fichier lui-même
		file_len = self.data_pth.stat().st_size
		self.data_len = self.data_pth.stat().st_size
		# taille d'un bloc, ajusté au block_boundary le plus proche, en général 8
		self.block_len = (((self.meta.sizeof // self.block_boundary) + 1) * self.block_boundary) if (self.meta.sizeof % self.block_boundary) != 0 else self.meta.sizeof
		# nombre de blocs
		self.block_nbr = file_len // self.block_len

		# truncated size (if truncation needed, else trunc_size = data_len)
		if (file_len % self.block_len) != 0 :
			print("\x1b[33mFile was truncated !\x1b[0m")
		self.data_len = self.block_nbr * self.block_len

		# truncated size (if truncation needed, else trunc_size = data_len)
		self.data_len = (file_len // self.block_nbr) * self.block_nbr if (file_len % self.block_len) != 0 else file_len
		

		print(f"LOADING data :: {self.data_pth}")
		
		start_clock = time.perf_counter_ns()

		if self.use_cache :
			self.data = None
			try :
				from structarray.rebin.cache import CacheRebin
				self.cache = CacheRebin(self.data_pth.with_suffix('.__cache__.hdf5'))
			except ModuleNotFoundError :
				self.cache = dict()

		with self.data_pth.open('rb') as fid :
			if self.use_mmap :
				self.data = mmap.mmap(fid.fileno(), self.data_len, prot=mmap.PROT_READ)
			else :
				# in all cases, self.data shall expose a buffer-like interface
				raise NotImplementedError("really ? use mmap !")

		stop_clock = time.perf_counter_ns()
		self.load_time = stop_clock - start_clock

		print(f" => file of {self.data_len} bytes" + (f" truncated to {self.data_len}" if self.data_len != file_len else "") + f" or {self.block_nbr} blocks of {self.block_len} bytes")

		return self

	def __len__(self) :
		return self.block_nbr
	
	def _read_buffer(self, name) :

		ctype, offset = self.meta[name]

		width = self.block_nbr
		height = self.data_len // (width * sizeof_map[ctype])

		if self.meta.is_aligned(name) :
			# les données sont alignées, on peut utiliser l'astuce ultime !
			arr = np.frombuffer(self.data, dtype=ntype_map[ctype])
			arr.shape = (width, height)
			return arr[:,int(offset) // sizeof_map[ctype]]
		else :
			# sinon il faut les ramasser une par une à la petite cuillère
			# print(f"{name} is not properly aligned ! offset={int(offset)} {int(offset) % sizeof_map[ctype]}")
			v_lst = list()
			pos = offset
			for i in range(width) :
				v = struct.unpack_from(stype_map[ctype], self.data, pos)[0]
				v_lst.append(v)
				pos += self.block_len
			v_arr = np.array(v_lst)
			return v_arr[:self.block_nbr]

	def __getitem__(self, name) :
		# print(f"__getitem__({name}) use_cache={self.use_cache}")
		if self.use_cache :
			if name not in self.cache :
				self.cache[name] = self._read_buffer(name)
			return self.cache[name]
		else :
			return self._read_buffer(name)

	def extract_structured_array(self, * c_lst) :
		""" return an array whose dtype is a flat structure of each column passed in c_lst """
		d_lst = [
			(f"{i:05d}_{c.split('.')[-1]}", ntype_map[self.meta.get_type(c)]) for i, c in enumerate(c_lst)
		]
		z = np.empty((len(self),), dtype=d_lst)
		for i, c in enumerate(c_lst) :
			z[f"{i:05d}_{c.split('.')[-1]}"] = self[c]

		return z
