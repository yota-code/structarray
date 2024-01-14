#!/usr/bin/env python3

import collections
import enum

sizeof_map = { # size of types
	'N1' : 1,
	'Z1' : 1,
	'N2' : 2,
	'Z2' : 2,
	'N4' : 4,
	'Z4' : 4,
	'R4' : 4,
	'N8' : 8,
	'Z8' : 8,
	'R8' : 8,
	'P4' : 4,
	'P8' : 8,
}

class MetaHandler() :
	
	def __init__(self) :
		self._m = collections.ordereddict()
		
	def load(self, pth) :

		self.pth = pth.resolve()
		if self.pth.suffix != '.tsv' :
			raise ValueError("mapping must be a tsv file")
		
		self._m = collections.ordereddict()
		self._v = list()

		obj = pth.load()
		
		line = obj.pop(0)
		self.name, self.sizeof = line[0], int(line[1])

		if self.format not in ["absolute", "relative", "compact"] :
			raise ValueError("mapping type unspecified")
		
		self._load(obj)

	def _load(self, obj) :
		is_relative = len(obj[0]) == 2 or ( len(obj[0]) == 3 and int(obj[0][3]) == 0 )

		addr = 0
		for line in obj :
			if len(line) == 2 :
				name, ctype, value = * line, 0
			elif len(line) == 3 :
				name, ctype, value = line[0], line[1], int(line[2])
			else :
				raise ValueError(f"malformed line, {line}")
			
			if '/' in name :
				c, sep, z = name.partition('/')
				name = prev.split('.')[:int(c)] + '.' + z

			if ctype in ['P4', 'P8'] :
				pass
			else :
				addr = addr if is_relative else value
				self._m[name] = (ctype, addr)

			addr += value + sizeof_map[ctype]

			prev = name

	def dump(self, is_relative=True, is_compact=False) :

		s_lst = list()
		p_lst = list()

		prev = 0
		for name, (ctype, addr) in self._m.items() :
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
				s_lst.append([name, ctype,])
				prev = addr

				padding = self.var_size - prev - int(s_lst[-1][1][1:])
				if padding != 0 :
					s_lst[-1].append(padding)
			else :
				s_lst.append([name, ctype, addr])

		return s_lst
			



