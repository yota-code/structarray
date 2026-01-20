#!/usr/bin/env python3

"""
le moyen le plus rapide de récupérer le .debug_info:
readelf -wi *.o
"""

import collections
import time
import warnings
import dataclasses

from cc_pathlib import Path
import sys

from structarray.meta import MetaReb

from elftools.elf.elffile import ELFFile

die_encoding_str = 'xyNwRZZNN'

if False :
	Base = collections.namedtuple('Base', ['name', 'mtype', 'msize'])
	Pointer = collections.namedtuple('Pointer', ['type', 'size'])
	Typedef = collections.namedtuple('Typedef', ['type', 'name'])
	Array = collections.namedtuple('Array', ['type', 'shape'])
	Structure = collections.namedtuple('Structure', ['size', 'detail'])
	Member = collections.namedtuple('Member', ['type', 'name', 'offset'])
	Variable = collections.namedtuple('Variable', ['type', 'name'])
else :
	@dataclasses.dataclass
	class Base() :
		name: str
		mtype: str
		msize: int

	@dataclasses.dataclass
	class Pointer() :
		type: int
		size: int

	@dataclasses.dataclass
	class Typedef() :
		type: int
		name: str

	@dataclasses.dataclass
	class Array() :
		type: int
		shape: tuple[int]

	@dataclasses.dataclass
	class Member() :
		type: int
		name: str
		offset: int

	@dataclasses.dataclass
	class Structure() :
		size: int
		detail: list[Member]

		def _to_json(self) :
			return {f"@Structure(size={self.size}, detail=...)" : self.detail}

	@dataclasses.dataclass
	class Variable() :
		type: int
		name: str

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
		'N4' : "uint32_t",
		'Z1' : "int8_t",
		'N1' : "uint8_t",
	}

	def __init__(self, elf_pth) :

		warnings.warn("this class is deprecated, please migrate to next ElfParser() in parseelf2", DeprecationWarning, stacklevel=2)

		self._time_lst = [time.time(),]
		
		self.top = self.load(elf_pth)
		self.chrono("elftools.get_top_DIE()")

		self.r_map = collections.defaultdict(dict)
		self.s_map = dict() # liste des symboles de haut niveau

		self.typedef_map = dict()
		self.variable_map = dict()
		self.base_map = dict()

		self.default_name = None

		for top in self.load(elf_pth) :
			self.parse(top)
			self.chrono(f"parse({top.attributes['DW_AT_name'].value.decode('utf8')})")

		self.chrono("parse()")

		Path("r_map.json").save(self.r_map, verbose=True)
		Path("s_map.json").save(self.s_map, verbose=True)

		Path("typedef_map.json").save(self.typedef_map, verbose=True)
		Path("variable_map.json").save(self.variable_map, verbose=True)
		Path("base_map.json").save(self.base_map, verbose=True)

	def run(self, name, mapping_pth, is_relative=True, is_compact=True) :
		
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
		print(f"get_ident {name}")
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
		self._time_lst.append(time.time())
		print(f"{self._time_lst[-1] - self._time_lst[-2]:7.3f} /{self._time_lst[-1] - self._time_lst[0]:7.3f} :: {label}") 

	def load(self, pth) :
		with Path(pth).open('rb') as fid :
			elffile = ELFFile(fid)
			self.chrono("elftools.ELFFile()")

			if not elffile.has_dwarf_info() :
				raise ValueError

			self.info = elffile.get_dwarf_info(relocate_dwarf_sections=False, follow_links=False)
			self.chrono("elftools.get_dwarf_info()")

		for unit in self.info.iter_CUs() :
			yield unit.get_top_DIE()

	def parse(self, top) :
		if not top.has_children :
			raise ValueError

		for i, child in enumerate(top.iter_children()) :
			tag = child.tag.removeprefix('DW_TAG_')
			func = f"_parse_{tag}"
			try :
				getattr(self, func)(child)
				if 'DW_AT_sibling' in child.attributes :
					self.s_map[child.attributes['DW_AT_sibling'].value] = child.offset
			except AttributeError :
				print(f"ERROR::{func}::{child}")

	def _parse_base_type(self, die) :
		p = Base(
			die.attributes['DW_AT_name'].value.decode('utf8'),
			die_encoding_str[die.attributes['DW_AT_encoding'].value],
			die.attributes['DW_AT_byte_size'].value
		)
		self.r_map[die.offset] = p
		self.base_map[p.name] = p.mtype

	def _parse_typedef(self, die) :
		p = Typedef(
			die.attributes['DW_AT_type'].value if 'DW_AT_type' in die.attributes else None,
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
		p = Pointer(
			die.attributes['DW_AT_type'].value if 'DW_AT_type' in die.attributes else None,
			die.attributes['DW_AT_byte_size'].value,
		)
		self.r_map[die.offset] = p


	def _parse_const_type(self, die) :
		if 'DW_AT_name' in die.attributes :
			p = Variable(
				die.attributes['DW_AT_type'].value,
				die.attributes['DW_AT_name'].value.decode('utf8')
			)
			self.r_map[die.offset] = p
			self.variable_map[p.name] = p.type

	def _parse_variable(self, die) :
		if 'DW_AT_name' in die.attributes :
			p = Variable(
				die.attributes['DW_AT_type'].value,
				die.attributes['DW_AT_name'].value.decode('utf8')
			)
			self.r_map[die.offset] = p
			self.variable_map[p.name] = p.type

	def _parse_volatile_type(self, die) :
		if 'DW_AT_name' in die.attributes :
			p = Variable(
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
					child.attributes['DW_AT_name'].value.decode('utf8') if ('DW_AT_name' in child.attributes) else None,
					child.attributes['DW_AT_data_member_location'].value,
				))
		p = Structure(
			die.attributes['DW_AT_byte_size'].value if 'DW_AT_byte_size' in die.attributes else None,
			m_lst
		)
		self.r_map[die.offset] = p

	def _parse_enumeration_type(self, die) :
		pass

	def _parse_union_type(self, die) :
		pass

	def _parse_subprogram(self, die) :
		pass

	def _parse_subroutine_type(self, die) :
		pass

if __name__ == "__main__" :

	elf_pth = Path(sys.argv[1])
	name = sys.argv[2].strip()
	u = ElfParser(elf_pth).run(name, Path("context.tsv"))
