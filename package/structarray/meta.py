#!/usr/bin/env python3

import ast
import collections
import math
import re

from cc_pathlib import Path

sizeof_map = { # size of types
	'N1' : 1,
	'N2' : 2,
	'N4' : 4,
	'N8' : 8,
	'Z1' : 1,
	'Z2' : 2,
	'Z4' : 4,
	'Z8' : 8,
	'R4' : 4,
	'R8' : 8,
	'P4' : 4,
	'P8' : 8,
}

ntype_map = { # types numpy
	'Z1' : "int8",
	'Z2' : "int16",
	'Z4' : "int32",
	'Z8' : "int64",
	'N1' : "uint8",
	'N2' : "uint16",
	'N4' : "uint32",
	'N8' : "uint64",
	'R4' : "float32",
	'R8' : "float64",
}

def compact_name(v_lst) :
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


def expand_name(r_lst) :
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

class MetaGeneric() :
	""" decrit les adresses d'un blob binaire
	TODO : tout remettre la dedans, seuls les loader changent suivant le format
	et même MetaParse devrait être ici...
	ou pas, on devrait avoir un parseur par type de données d'entrées
	"""
	def __getitem__(self, key) :
		return self._m[key]

class MetaReb(MetaGeneric) :
	""" un gestionnaire des méta données pour les enregistrements .reb """
	
	def __init__(self, name=None, sizeof=None) :
		self._m = collections.OrderedDict()

		self.name = name
		self.sizeof = sizeof

	def push(self, name, mtype, addr) :
		self._m[name] = (mtype, addr)

	def __iter__(self) :
		for name, (mtype, addr) in self._m.items() :
			if not mtype.startswith('P') :
				yield name
		
	def load(self, pth) :
		pth = Path(pth).resolve()

		if pth.suffix != '.tsv' :
			raise ValueError("mapping must be a tsv file")
		if not pth.is_file() :
			raise FileNotFoundError(f"{pth} does not exists")
		
		self._m = collections.OrderedDict()

		obj = pth.load()
		
		line = obj.pop(0)
		self.name, self.sizeof = line[0], int(line[1])
		
		self._load_addr(obj)

		return self

	def _load_addr(self, obj) :
		""" si la première ligne des addresses ne contient que 2 champs,
		on considère que c'est un fichier décrit en relatif """
		is_relative = len(obj[0]) == 2

		addr = 0
		for line in obj :
			if len(line) == 2 :
				name, mtype, value = * line, 0
			elif len(line) == 3 :
				name, mtype, value = line[0], line[1], int(line[2])
			else :
				raise ValueError(f"malformed line, {line}")
			
			if '/' in name :
				# si y a un / c'est que le nom est compact
				c, sep, z = name.partition('/')
				try :
					name = '.'.join(prev.split('.')[:int(c)]) + '.' + z
				except :
					print(prev, z)
					print(prev.split('.')[:int(c)])
					raise ValueError

			addr = addr if is_relative else value
			self._m[name] = (mtype, addr)

			addr += value + sizeof_map[mtype]

			prev = name

	def dump(self, pth, is_relative=True, is_compact=False) :
		pth = Path(pth).resolve()

		if not pth.suffix == '.tsv' :
			raise ValueError
		if self.sizeof is None :
			raise ValueError("self.sizeof is not defined !")
		
		s_lst = [
			[self.name, self.sizeof],
		]
		s_lst += self._dump_addr(is_relative, is_compact)

		pth.save(s_lst)

	def _dump_addr(self, is_relative, is_compact) :

		s_lst = list()
		p_lst = list()

		prev = 0
		for name, (mtype, addr) in self._m.items() :
			if is_compact :
				n_lst = name.split('.')
				q = 0
				for p, n in zip(p_lst, n_lst) :
					if p != n :
						break
					q += 1
				name = (f"{q}/" if q else '') + '.'.join(n_lst[q:])
				p_lst = n_lst
			if is_relative :
				element_nbr = int(re.match(r'.*?\[(?P<size>\d+)\]', name).group('size')) if name.endswith(']') else 1

				if s_lst :
					padding = addr - prev
					# assert 0 <= padding < 7
					if padding != 0 :
						s_lst[-1].append(padding)

				s_lst.append([name, mtype,])

				element_size = int(s_lst[-1][1][1:])
				prev = addr + (element_size * element_nbr)
			else :
				s_lst.append([name, mtype, addr])

		if is_relative :
			padding = self.sizeof - prev
			if padding != 0 :
				s_lst[-1].append(padding)

		return s_lst
		
	def __eq__(self, other) :
		for self_line, other_line  in zip(self._m.items(), other._m.items()) :
			if self_line != other_line :
				print("SELF ", self_line)
				print("OTHER", other_line)
				return False
		return True
	
	def is_aligned(self, name) :
		ctype, offset = self[name]
		return offset % sizeof_map[ctype] == 0

	def all_aligned(self) :
		for name in self._m :
			ctype, offset = self._m[name]
			if offset % sizeof_map[ctype] != 0 :
				print(f"{name} is not aligned: size={sizeof_map[ctype]} offeset={offset}")
				return False
		return True
	
	def search(self, pattern, mode='blob') :
		# print(f"StructArray.search({pattern}, {mode})")
		if mode == 'blob':
			pattern = pattern.replace('.', '\\.').replace('*', '.*')
		elif mode == 'regexp' :
			pass
		rec = re.compile(pattern, re.IGNORECASE | re.ASCII)
		return [var for var in self if rec.search(var) is not None]


def expand_name_gen() :
	# validated
	# undo the compaction and give back the original names
	r = yield None
	while True :
		if '/' in r :
			c, sep, z = r.partition('/')
			n_lst = p_lst[:int(c)] + z.split('.')
			r = yield '.'.join(n_lst)
		else :
			n_lst = r.split('.')
			r = yield r
		p_lst = n_lst


z_map = {
	'inf' : math.inf,
	'nan' : math.nan,
	'-inf' : -math.inf
}

class MetaRez(MetaGeneric) :
	def __init__(self) :
		self._m = collections.OrderedDict()

	def load(self, meta_zip) :
		import brotli

		self._m.clear()

		meta_bin = brotli.decompress(meta_zip)
		meta_txt = meta_bin.decode('ascii')
		meta_lst = meta_txt.splitlines()

		self.array_len = int(meta_lst.pop(0))

		exp = expand_name_gen()
		next(exp)

		for line in meta_lst :
			r, value = line.split('\t')
			v = exp.send(r)
			m = value[:2]
			z = value[2]
			try :
				b = ast.literal_eval(value[3:])
			except ValueError :
				try :
					b = float(value[3:])
				except :
					raise
			self._m[v] = (m, z, b)

		print('\n'.join(list(self._m)[:20]))

		return self
