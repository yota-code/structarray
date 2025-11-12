#!/usr/bin/env python3

import collections
import hashlib
import io
import math
import mmap
import os
import re
import struct

import numpy as np

from cc_pathlib import Path

import structarray.meta
from structarray.common import *

class DataHandler() :
	"""
	this class aims to handle a single .reb file.
	a .reb file is the native structarray file, it only consists in raw C structures dumped directly and successively into a file
	in order to access fields by name, a MetaReb instance must be provided
	"""

	use_cache = False
	use_mmap = True

	def __init__(self, data_pth, meta=None) :

		data_pth = Path(data_pth).resolve(strict=True)

		match meta :
			case Path() | str() :
				self.meta = structarray.meta.get_meta[meta]
			case structarray.meta.MetaHandler() :
				self.meta = meta
			case _ :
				for k in ["context_map.tsv", "compact_map.tsv"] :
					pth = data_pth.parent / k
					if pth.is_file() :
						self.meta = structarray.meta.get_meta[pth]

		self.load(data_pth)
		
	def __len__(self) :
		return self.vector_len
	
	def load(self, data_pth) :
		self.data_pth = Path(data_pth).resolve()

		assert self.data_pth.suffix == '.reb'

		# taille du fichier lui-même
		self.data_len = self.data_pth.stat().st_size
		# taille d'un bloc
		self.block_len = (((self.meta.sizeof // 8) + 1) * 8) if (self.meta.sizeof % 8) != 0 else self.meta.sizeof
		# nombre de blocs
		self.vector_len = self.data_len // self.meta.sizeof

		print(f"LOADING DATA :: {self.data_pth} => {self.data_len} bytes or {self.vector_len} blocks of {self.block_len} bytes\n")

		assert self.data_len % self.block_len == 0

		if self.use_cache :
			self.data = None
			try :
				from structarray.cache import CacheHandler
				self.cache = CacheHandler(self.data_pth.with_suffix('.__cache__.hdf5'))
			except ModuleNotFoundError :
				self.cache = dict()
		else :
			with self.data_pth.open('rb') as fid :
				if self.use_mmap :
					self.data = mmap.mmap(fid.fileno(), 0, prot=mmap.PROT_READ)
				else :
					# in all cases, self.data shall expose a buffer-like interface
					raise NotImplementedError("really ? use mmap !")

		return self

	def _read_buffer(self, name) :

		ctype, offset = self.meta[name]

		if self.meta.is_aligned(name) :
			# les données sont alignées, on peut utiliser l'astuce ultime !
			width = self.data_len // self.block_len
			height = self.data_len // (width * sizeof_map[ctype])

			arr = np.frombuffer(self.data, dtype=ntype_map[ctype])
			arr.shape = (width, height)

			return arr[:, int(offset) // sizeof_map[ctype]]
		else :
			# sinon il faut les ramasser une par une à la petite cuillère
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

	def to_rez(self) :

		""" en deux passes ? la première repère les vecteurs constants ou identiques 
		la deuxième fourre tout dans un hdf5 ? mais ça fait lire le fichier 2 fois
		
		ou alors on stocke dans des fichiers temporaires pour chaque type

		"""

		import brotli

		import h5py
		try :
			import hdf5plugin
		except ImportError :
			pass

		archive_pth = self.data_pth.with_suffix('.rez')

		try :
			import hdf5plugin

			h5py_opt = dict(
				hdf5plugin.Blosc2(cname='zstd', clevel=9, filters=hdf5plugin.Blosc2.SHUFFLE | hdf5plugin.Blosc2.DELTA)
			)
		except :
			h5py_opt = {
				'compression' : "gzip",
				'compression_opts' : 9,
				'shuffle' : True,
				# 'fletcher32' : True,
			}

		print(h5py_opt)

		v_lst = list(self.meta)
		r_lst = compact_name(v_lst)

		# assert len(v_lst) == len(r_lst)

		if archive_pth.is_file() :
			archive_pth.unlink()

		e_map = dict()
		for c in ['R8', 'R4', 'Z4', 'Z2', 'Z1', 'N4', 'N2', 'N1'] :
			i_lst = [i for i, v in enumerate(v_lst) if self.meta[v][0] == c]
			print(f"{c} {0:7d} / {len(i_lst)}")
			if i_lst :
				e_map[c] = dict() # ctype -> name -> position
				s = collections.defaultdict(set) # hash -> position set
				m = list()
				for n, i in enumerate(i_lst) :
					v = v_lst[i] # full name of the variable
					d = self[v] # full data line extracted
					if d[0] == d[-1] and (d[0] == d).all() :
						e_map[c][i] = ('=', d[0])
						print(f"\x1b[A\x1b[K{c} {n+1:7d} / {len(i_lst)} # {d[0]} = {v_lst[i]}")
					else :
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
						e_map[c][i] = ('@', j)
						print(f"\x1b[A\x1b[K{c} {n+1:7d} / {len(i_lst)} # {j} @ {v_lst[i]}")

				Path(f"s_map.{c}.json").save(s)

				with h5py.File(archive_pth, 'a', libver="latest") as obj :
					w = np.vstack(m)
					print(f"\x1b[A\x1b[K{c} {len(i_lst):7d} / {len(i_lst)} => {w.shape[0]} rows")
					print("RAAAH", w.shape)
					obj.create_dataset('/' + c, data=w, ** h5py_opt)

		f_lst = [str(self.vector_len),] # on doit garder vector_len dans les méta données parce qu'il se peut que TOUS les vecteurs soient constants
		for i, (v, r) in enumerate(zip(v_lst, r_lst)) :
			mtype = self.meta[v][0]
			z, j = e_map[mtype][i]
			f_lst.append(f'{r}\t{mtype}{z}{j}')

		Path(archive_pth.with_suffix('.mez')).write_text('\n'.join(f_lst))

		with h5py.File(archive_pth, 'a', libver="latest") as obj :
			meta_zip = brotli.compress('\n'.join(f_lst).encode('ascii'), mode=brotli.MODE_TEXT)
			obj.attrs['_meta'] = np.void(meta_zip) # https://docs.h5py.org/en/stable/strings.html

		data_size = self.data_pth.stat().st_size
		meta_size = self.meta_pth.stat().st_size
		archive_size = archive_pth.stat().st_size
		print(f"\noriginal: {data_size + meta_size:15d} bytes ({meta_size:8d} meta)\n archive: {archive_size:15d} bytes ({len(meta_zip):8d} meta)\n => archive takes {100.0 * archive_size / (data_size + meta_size):0.5}% of original")

		return archive_pth