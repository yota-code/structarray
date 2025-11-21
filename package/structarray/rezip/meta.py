#!/usr/bin/env python3

import ast
import collections

from structarray.meta import MetaGeneric

class MetaRezip(MetaGeneric) :
	def __init__(self) :
		self._m = collections.OrderedDict()

	def load(self, meta_zip:bytes) :
		import brotli

		self._m.clear()

		meta_bin = brotli.decompress(meta_zip)
		meta_txt = meta_bin.decode('ascii')
		meta_lst = meta_txt.splitlines()

		self.array_len = int(meta_lst.pop(0))

		exp = self._proc_name_expand()
		next(exp)

		for line in meta_lst :
			r, value = line.split('\t')
			v = exp.send(r)
			if value.startswith('P') :
				continue
			m = value[:2]
			z = value[2]
			try :
				b = ast.literal_eval(value[3:])
			except ValueError :
				try :
					b = float(value[3:])
				except :
					raise
			except SyntaxError :
				print(value)
				raise
			self._m[v] = (m, z, b)

		return self
