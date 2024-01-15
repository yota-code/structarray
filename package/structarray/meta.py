#!/usr/bin/env python3

import collections
import enum
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

class MetaHandler() :
	
	def __init__(self) :
		self._m = collections.OrderedDict()

		self.name = None
		self.sizeof = None

	def __iter__(self) :
		for name, (mtype, addr) in self._m.items() :
			if not mtype.startswith('P') :
				yield name

	def __getitem__(self, key) :
		return self._m[key]
		
	def load(self, pth) :
		pth = Path(pth).resolve()

		if pth.suffix != '.tsv' :
			raise ValueError("mapping must be a tsv file")
		if not pth.is_file() :
			raise FileNotFoundError(f"{pth} does not exists")
		
		self._m = collections.OrderedDict()
		self._v = list()

		obj = pth.load()
		
		line = obj.pop(0)
		self.name, self.sizeof = line[0], int(line[1])
		
		self._load_addr(obj)

		return self

	def _load_addr(self, obj) :
		is_relative = len(obj[0]) == 2 # or ( len(obj[0]) == 3 and int(obj[0][2]) == 0 )

		addr = 0
		for line in obj :
			if len(line) == 2 :
				name, mtype, value = * line, 0
			elif len(line) == 3 :
				name, mtype, value = line[0], line[1], int(line[2])
			else :
				raise ValueError(f"malformed line, {line}")
			
			if '/' in name :
				c, sep, z = name.partition('/')
				try :
					name = '.'.join(prev.split('.')[:int(c)]) + '.' + z
				except :
					print(prev, z)
					print(prev.split('.')[:int(c)])
					raise

			addr = addr if is_relative else value
			self._m[name] = (mtype, addr)

			addr += value + sizeof_map[mtype]

			prev = name

	def dump(self, pth, is_relative=True, is_compact=False) :
		pth = Path(pth).resolve()

		if not pth.suffix == '.tsv' :
			raise ValueError
		if self.sizeof is None :
			raise ValueError
		
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
				if s_lst :
					padding = addr - prev - int(s_lst[-1][1][1:])
					if padding != 0 :
						s_lst[-1].append(padding)
				s_lst.append([name, mtype,])
				prev = addr
			else :
				s_lst.append([name, mtype, addr])

		if is_relative :
			padding = self.sizeof - prev - int(s_lst[-1][1][1:])
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
