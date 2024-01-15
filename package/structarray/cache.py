#!/usr/bin/env python3

import hashlib

import h5py

class CacheHandler() :

	""" simple cache function, with a single optimisation :
	identical lines will not be duplicated
	"""

	def __init__(self, cache_pth) :

		self.hdf_pth = cache_pth.resolve()
		# self.hsh_pth = self.hdf_pth.with_suffix(".tsv")

		self.key_map = dict() # key -> hsh
		self.hsh_map = dict() # hsh -> key

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
			return obj[self.key_map[key]][...]

	def __setitem__(self, key, value) :
		hsh = hashlib.blake2b(value.tobytes(), digest_size=24).hexdigest()

		with h5py.File(self.hdf_pth, 'a', libver="latest") as obj :
			if hsh not in self.hsh_map :
				obj.create_dataset('/' + hsh, data=value, ** self.h5py_opt)
				obj.attrs[key] = hsh

		self.key_map[key] = hsh
		self.hsh_map[hsh] = key
			
	def __contains__(self, key) :
		return key in self.key_map

