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

from structarray.common import *

class DataRebin() :
	"""
	this class aims to handle a single .reb file.
	a .reb file is the native structarray file, it only consists in raw C structures dumped directly and successively into a file
	in order to access fields by name, a MetaReb instance must be provided
	"""

	use_cache = False
	use_mmap = True

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

		match meta :
			case Path() | str() :
				self.meta = structarray.rebin.meta._cached_meta[Path(meta).resolve(strict=True)]
			case structarray.rebin.meta.MetaRebin() :
				self.meta = meta
			case _ :
				# si meta n'est pas passé on essaie d'ouvrir un fichier meta à côté du fichier data
				for k in ["context_map.tsv", "compact_map.tsv"] :
					pth = (self.data_pth.parent / k).resolve()
					if pth.is_file() :
						self.meta = structarray.rebin.meta._cached_meta[pth]
						break

		self._load_data()
		
	def _load_data(self) :

		assert self.data_pth.suffix == '.reb'

		# taille du fichier lui-même
		self.data_len = self.data_pth.stat().st_size
		# taille d'un bloc
		self.block_len = (((self.meta.sizeof // 8) + 1) * 8) if (self.meta.sizeof % 8) != 0 else self.meta.sizeof
		# nombre de blocs
		self.vector_len = self.data_len // self.meta.sizeof

		print(f"LOADING data :: {self.data_pth}")

		assert self.data_len % self.block_len == 0

		start_clock = time.perf_counter_ns()

		if self.use_cache :
			self.data = None
			try :
				from structarray.rebin.cache import CacheRebin
				self.cache = CacheRebin(self.data_pth.with_suffix('.__cache__.hdf5'))
			except ModuleNotFoundError :
				self.cache = dict()
		else :
			with self.data_pth.open('rb') as fid :
				if self.use_mmap :
					self.data = mmap.mmap(fid.fileno(), 0, prot=mmap.PROT_READ)
				else :
					# in all cases, self.data shall expose a buffer-like interface
					raise NotImplementedError("really ? use mmap !")

		stop_clock = time.perf_counter_ns()
		self.load_time = stop_clock - start_clock

		print(f" => {self.data_len} bytes or {self.vector_len} blocks of {self.block_len} bytes")

		return self

	def __len__(self) :
		return self.vector_len
	
	def _read_buffer(self, name) :

		ctype, offset = self.meta[name]

		width = self.data_len // self.block_len
		height = self.data_len // (width * sizeof_map[ctype])

		if self.meta.is_aligned(name) :
			# les données sont alignées, on peut utiliser l'astuce ultime !
			arr = np.frombuffer(self.data, dtype=ntype_map[ctype])
			arr.shape = (width, height)
			return arr[:, int(offset) // sizeof_map[ctype]]
		else :
			# sinon il faut les ramasser une par une à la petite cuillère
			# print(f"{name} is not properly aligned ! offset={int(offset)} {int(offset) % sizeof_map[ctype]}")
			v_lst = list()
			pos = offset
			for i in range(width) :
				v = struct.unpack_from(stype_map[ctype], self.data, pos)[0]
				v_lst.append(v)
				pos += self.block_len
			return np.array(v_lst)

	def __getitem__(self, name) :
		# print(f"__getitem__({name})")
		if self.use_cache :
			if name not in self.cache :
				self.cache[name] = self._read_buffer(name)
			return self.cache[name]
		else :
			return self._read_buffer(name)

