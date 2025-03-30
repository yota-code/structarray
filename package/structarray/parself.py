#!/usr/bin/env python3

"""
le moyen le plus rapide de récupérer le .debug_info:
readelf -wi *.o
"""

import collections
import time

from cc_pathlib import Path
import sys

from structarray.meta import MetaReb

from elftools.elf.elffile import ELFFile

die_encoding_str = 'xyNwRZZNN'

Base = collections.namedtuple('Base', ['name', 'mtype'])
Pointer = collections.namedtuple('Pointer', ['type', 'size'])
Typedef = collections.namedtuple('Typedef', ['type', 'name'])
Array = collections.namedtuple('Array', ['type', 'shape'])
Structure = collections.namedtuple('Structure', ['size', 'detail'])
Member = collections.namedtuple('Member', ['type', 'name', 'offset'])
Variable = collections.namedtuple('Variable', ['type', 'name'])

class ElfParser() :
	"""
	TODO:
	    si on a un tableau de structure, y a des trucs qui pourraient
	    passer à l'as dans metaReb lors de la génération des adresses
	    compactes

	    il manque un arbre à parcourir... je sais pas où le mettre !
	    meta reb à vocation d'être utilisé juste pour le parsing rapide,
	    mais est-ce ompatible avec le point du dessus?

		Sauf si on arrive à se passer de représentation en arbre par la suite
		IL FAUT une classe générique pour les infos de structures... à réfléchir
	"""

	mtype = {
		'R8' : "double",
		'R4' : "float",
		'Z4' : "int32_t",
		'N1' : "uint8_t",
		'Z1' : "int8_t",
	}

	def __init__(self, elf_pth) :
		self.t_lst = [time.time(),]
		
		self.top = self.load(elf_pth)
		self.chrono("elftools.get_top_DIE()")

		self.r_map = collections.defaultdict(dict)
		self.s_map = dict() # liste des symboles de haut niveau

		self.typedef_map = dict()
		self.variable_map = dict()
		self.base_map = dict()

		self.default_name = None

		self.parse()
		self.chrono("parse()")

	def run(self, name, mapping_pth, is_relative=True, is_compact=True) :

		save_dir = mapping_pth.parent
		
		(save_dir / "r_map.json").save(self.r_map, verbose=True)
		(save_dir / "s_map.json").save(self.s_map, verbose=True)

		(save_dir / "typedef_map.json").save(self.typedef_map, verbose=True)
		(save_dir / "variable_map.json").save(self.variable_map, verbose=True)
		(save_dir / "base_map.json").save(self.base_map, verbose=True)

		self.default_name = name

		def as_array(shape) :
			return ''.join(f'[{s}]' for s in shape) if isinstance(shape, tuple) else ''

		u = MetaReb(name)

		for m_lst in self.walk(name) :
			# print(m_lst)
			p_lst = [m[0] for m in m_lst[:-1] if m[0] is not None]
			p_lst.append(m_lst[-1][0] + as_array(m_lst[-1][1]))
			u.push('.'.join(p_lst), m_lst[-1][2], m_lst[-1][3])
		u.sizeof = self.sizeof
		self.chrono("dump()")

		u.dump(mapping_pth.with_suffix(".debug.tsv"), False, False)
		u.dump(mapping_pth, is_relative, is_compact)

		mapping_pth.with_suffix(".debug.json").save(u._m, verbose=True)
		
		self.chrono("total()")

		return u

	def get_ident(self, name) :
		# name can either be the name of a global variable or a the name of a typedef
		if name in self.variable_map :
			pident = self.variable_map[name]
		elif name in self.typedef_map :
			pident = self.typedef_map[name]
		else :
			raise ValueError

		if pident in self.s_map :
			# print(f"S_MAP {pident} -> {self.s_map[pident]}")
			pident = self.s_map[pident]
		while pident in self.r_map and isinstance(self.r_map[pident], (Typedef, Pointer)) :
			# print(f"R_MAP {pident} -> {self.r_map[pident][0]}")
			pident = self.r_map[pident].type
		
		return pident

	def walk(self, name=None, max_depth=None, follow_pointer=False) :
		pident = self.get_ident(self.default_name if name is None else name)

		self.sizeof = self.r_map[pident].size

		yield from self._walk(pident, list(), max_depth, follow_pointer)

	def to_base(self, q) :
		while isinstance(q, Typedef) :
			q = self.r_map[q.type]
		return q

	def _walk(self, pident, m_lst, max_depth, follow_pointer, depth=0) : 
		if m_lst :
			pname, pcount, ptype, poffset = m_lst[-1]
		else :
			pname, pcount, ptype, poffset = None, None, None, 0

		q = self.r_map[pident]

		match q :
			case Base() :
				m_lst[-1] = (pname, 1, q.mtype, poffset)
				yield m_lst
			case Typedef() :
				# print("TYPEDEF", pname, depth, max_depth, max_depth is None or depth <= max_depth)
				m_lst[-1] = (pname, 1, q.name, poffset)
				yield from self._walk(q.type, m_lst, max_depth, depth+1)
			case Structure() :
				# print("STRUCT ", pname, depth, max_depth, max_depth is None or depth <= max_depth)
				if max_depth is None or depth <= max_depth :
					for m in self.r_map[pident].detail :
						yield from self._walk(m.type, m_lst + [(m.name, 1, None, poffset + m.offset),], max_depth, follow_pointer, depth+1)
				else :
					yield m_lst
			case Pointer() :
				# self.r_map[q.type].name
				if follow_pointer :
					while pident in self.r_map and isinstance(self.r_map[pident], (Typedef, Pointer)) :
						# print(f"R_MAP {pident} -> {self.r_map[pident][0]}")
						pident = self.r_map[pident].type
					for m in self.r_map[pident].detail :
						yield from self._walk(m.type, m_lst + [(m.name + '*', 1, None, poffset + m.offset),], max_depth, follow_pointer, depth+1)
				else :
					m_lst[-1] = (pname, 0, f"P{q.size}", poffset)
					yield m_lst
			case Array() :
				m_lst[-1] = (pname, q.shape, self.to_base(self.r_map[q.type]).mtype, poffset)
				yield m_lst
			case _ :
				raise ValueError(m_lst, q)

	def chrono(self, label) :
		self.t_lst.append(time.time())
		print(f"{self.t_lst[-1] - self.t_lst[-2]:7.3f} /{self.t_lst[-1] - self.t_lst[0]:7.3f} :: {label}") 

	def load(self, pth) :
		with Path(pth).open('rb') as fid :
			elffile = ELFFile(fid)
			self.chrono("elftools.ELFFile()")

			if not elffile.has_dwarf_info() :
				raise ValueError

			self.info = elffile.get_dwarf_info(relocate_dwarf_sections=False, follow_links=False)
			self.chrono("elftools.get_dwarf_info()")

		for unit in self.info.iter_CUs() :
			print(unit, unit.get_top_DIE())
			return unit.get_top_DIE()

	def parse(self) :
		if not self.top.has_children :
			raise ValueError

		for i, child in enumerate(self.top.iter_children()) :
			tag = child.tag.removeprefix('DW_TAG_')
			func = f"_parse_{tag}"
			try :
				getattr(self, func)(child)
				if 'DW_AT_sibling' in child.attributes :
					self.s_map[child.attributes['DW_AT_sibling'].value] = child.offset
			except (AttributeError, KeyError) :
				print(f"ERROR::{func}::{child}")


		Path("r_map.json").save(self.r_map, verbose=True)
		Path("s_map.json").save(self.s_map, verbose=True)

		Path("typedef_map.json").save(self.typedef_map, verbose=True)
		Path("variable_map.json").save(self.variable_map, verbose=True)
		Path("base_map.json").save(self.base_map, verbose=True)

	def _parse_base_type(self, die) :
		p = Base(
			die.attributes['DW_AT_name'].value.decode('utf8'),
			f"{die_encoding_str[die.attributes['DW_AT_encoding'].value]}{die.attributes['DW_AT_byte_size'].value}"
		)
		self.r_map[die.offset] = p
		self.base_map[p.name] = p.mtype

	def _parse_typedef(self, die) :
		p = Typedef(
			die.attributes['DW_AT_type'].value,
			die.attributes['DW_AT_name'].value.decode('utf8')
		)
		self.r_map[die.offset] = p
		self.typedef_map[p.name] = p.type

	def _parse_array_type(self, die) :
		u_lst = list()
		for i, child in enumerate(die.iter_children()) :
			if 'DW_AT_upper_bound' in child.attributes :
				u_lst.append(child.attributes['DW_AT_upper_bound'].value + 1)
			else :
				print(child)
		p = Array(
			die.attributes['DW_AT_type'].value,
			tuple(u_lst),
		)
		self.r_map[die.offset] = p

	def _parse_pointer_type(self, die) :
		print("RAAAH", die, die.attributes)
		p = Pointer(
			die.attributes['DW_AT_type'].value,
			die.attributes['DW_AT_byte_size'].value,
		)
		self.r_map[die.offset] = p

	def _parse_variable(self, die) :
		if 'DW_AT_name' in die.attributes :
			p = Typedef(
				die.attributes['DW_AT_type'].value,
				die.attributes['DW_AT_name'].value.decode('utf8')
			)
			self.r_map[die.offset] = p
			self.variable_map[p.name] = p.type

	def _parse_structure_type(self, die) :
		m_lst = list()
		for i, child in enumerate(die.iter_children()) :
			if child.tag == 'DW_TAG_member' :
				m_lst.append(Member(
					child.attributes['DW_AT_type'].value,
					child.attributes['DW_AT_name'].value.decode('utf8'),
					child.attributes['DW_AT_data_member_location'].value,
				))
		p = Structure(
			die.attributes['DW_AT_byte_size'].value,
			m_lst
		)
		self.r_map[die.offset] = p


	def _parse_subprogram(self, die) :
		# on veut pas traiter les sous programmes
		pass