#!/usr/bin/env python3

"""
le moyen le plus rapide de récupérer le .debug_info:
readelf -wi *.o
"""

import collections
import sys
import time

from cc_pathlib import Path

from structarray.meta import MetaReb

from elftools.elf.elffile import ELFFile

die_encoding_str = 'xyNwRZZNN'

if False :
	Base = collections.namedtuple('Base', ['name', 'letter', 'sizeof'])
	Pointer = collections.namedtuple('Pointer', ['oid', 'sizeof'])
	Typedef = collections.namedtuple('Typedef', ['oid', 'alias'])
	Array = collections.namedtuple('Array', ['oid', 'shape'])
	Structure = collections.namedtuple('Structure', ['sizeof', 'detail'])
	Member = collections.namedtuple('Member', ['oid', 'name', 'offset'])
	Variable = collections.namedtuple('Variable', ['oid', 'name'])
else :
	import dataclasses

	@dataclasses.dataclass
	class Base() :
		name: str
		letter: str
		sizeof: int

	@dataclasses.dataclass
	class Pointer() :
		oid: int
		sizeof: int

	@dataclasses.dataclass
	class Typedef() :
		oid: int
		alias: str

	@dataclasses.dataclass
	class Array() :
		oid: int
		shape: tuple[int]

	@dataclasses.dataclass
	class Member() :
		oid: int
		name: str
		offset: int

	@dataclasses.dataclass
	class Structure() :
		sizeof: int
		detail: list[Member]

		def _to_json(self) :
			return {f"@Structure(size={self.sizeof}, detail=...)" : self.detail}

		def __str__(self) :
			return f"Structure(size={self.sizeof}, detail=...)"
			# return f"Structure(size={self.sizeof}, detail={", ".join(str(m.oid) for m in self.detail)})"

		__repr__ = __str__

	@dataclasses.dataclass
	class Variable() :
		oid: int
		name: str

class ElfParser() :

	mtype = {
		'R8' : "double",
		'R4' : "float",
		'Z4' : "int32_t",
		'N4' : "uint32_t",
		'Z1' : "int8_t",
		'N1' : "uint8_t",
	}

	def __init__(self, elf_pth) :
		self._time_lst = [time.time(),]

		# self.u = TypeTree()
		
		self.r_map = collections.defaultdict(dict)
		self.s_map = dict() # liste des symboles de haut niveau

		self.typedef_map = dict()
		self.variable_map = dict()
		self.base_map = dict()

		self.default_name = None

		for top in self.load(elf_pth) :
			self.parse(top)
			self.chrono(f"parse(\x1b[33m{top.attributes['DW_AT_name'].value.decode('utf8')}\x1b[0m)")

		Path("r_map.json").save(self.r_map, verbose=True)
		Path("s_map.json").save(self.s_map, verbose=True)

		w_lst = list()
		for i, m_lst in enumerate(self.walk('_C_MfcAfcs')) :
			w_lst.append(self.expand(m_lst))
			if i > 100 :
				break
		Path("walk.txt").write_text('\n'.join(w_lst))

		w_lst = list()
		for i, m_lst in enumerate(self.walk('_C_MfcAfcs')) :
			w_lst.append(' > '.join(str(m) for m in m_lst))
			if i > 100 :
				break
		Path("walk.brut").write_text('\n'.join(w_lst))

		w_lst = list()
		for i, m_lst in enumerate(self.walk('_C_MfcAfcs')) :
			w_lst.append(self.expand_struct(m_lst))
			if i > 100 :
				break
		Path("walk.tsv").save(w_lst)

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

	def resolve(self, name_or_oid) :

		if isinstance(name_or_oid, str) :
			name = name_or_oid
			# name can either be the name of a global variable or a the name of a typedef
			if name in self.variable_map :
				oid = self.variable_map[name]
			elif name in self.typedef_map :
				oid = self.typedef_map[name]
			else :
				raise ValueError
		elif isinstance(name_or_oid, int) :
			oid = name_or_oid

		if oid in self.s_map :
			# print(f"S_MAP {oid} -> {self.s_map[oid]}")
			oid = self.s_map[oid]
		while oid in self.r_map and isinstance(self.r_map[oid], (Typedef, Pointer)) :
			# print(f"R_MAP {pident} -> {self.r_map[pident][0]}")
			oid = self.r_map[oid].oid

		print(f">>> resolve({name_or_oid}) -> {oid}/{self.r_map[oid]}")
		
		return oid

	def to_base(self, q) :
		while isinstance(q, Typedef) :
			q = self.r_map[q.type]
		return q

	def expand(self, m_lst) :
		# take a m_lst, return the path to the object

		s_lst = list()
		for oid, obj, offset in m_lst :
			if obj is None :
				obj = self.r_map[oid]
			match obj :
				case Base() :
					s_lst.append(f"[{obj.letter}{obj.sizeof}] @{offset}")
				case Typedef() :
					s_lst.append(f"{oid}!{obj.alias}")
				case Member() :
					s_lst.append(f"{oid}/{obj.name} @{obj.offset}")
				case _ :
					s_lst.append(f"{oid}#{obj.__class__.__name__}")

		return " > ".join(s_lst)

	def expand_struct(self, m_lst) :
		# take a m_lst, return the path to the object

		s_lst = list()
		for oid, obj, offset in m_lst :
			if isinstance(obj, Member) :
				s_lst.append(obj.name)

		oid, obj, offset = m_lst[-1]
		if isinstance(obj, Base) :
			return ".".join(s_lst), f"{obj.letter}{obj.sizeof}", offset
		else :
			return ".".join(s_lst), f"P{obj.sizeof}", offset

	def walk(self, name, max_depth=None, follow_pointer=False) :

		Path("walk.raw").write_text('')
		oid = self.resolve(name)
		m_lst = [(oid, self.r_map[oid], 0),]

		yield from self._walk(m_lst, max_depth, follow_pointer)

	def _walk(self, m_lst, max_depth, follow_pointer, depth=0) :
		
		print("\t" + "-" * (depth+1) + "> " + f"{self.expand(m_lst)}", max_depth, depth)
		with Path("walk.raw").open('at') as fid :
		 	fid.write(str(m_lst) + '\n')

		oid, obj, offset = m_lst[-1]

		match obj :
			case Base() :
				yield m_lst
			case Typedef() :
				yield from self._walk(m_lst + [(obj.oid, self.r_map[obj.oid], offset),], max_depth, follow_pointer, depth)
			case Member() :
				yield from self._walk(m_lst + [(obj.oid, self.r_map[obj.oid], offset),], max_depth, follow_pointer, depth+1)
			case Structure() :
				if max_depth is None or depth <= max_depth :
					for i, sub in enumerate(self.r_map[oid].detail) :
						yield from self._walk(m_lst + [(f"{i}", sub, offset + sub.offset),], max_depth, follow_pointer, depth)
				else :
					yield m_lst
			case Pointer() :
				if follow_pointer :
					yield from self._walk(m_lst + [(obj.oid, self.r_map[obj.oid], offset),], max_depth, follow_pointer, depth)
				else :
					yield m_lst

	def walk_smart(self, name=None, max_depth=None, follow_pointer=False) :
		pident = self.get_oid(self.default_name if name is None else name)

		self.sizeof = self.r_map[pident].size

		yield from self._walk_smart(pident, list(), max_depth, follow_pointer)

	def _walk_smart(self, pident, m_lst, max_depth, follow_pointer, depth=0) : 
		if m_lst :
			pname, pcount, ptype, poffset = m_lst[-1]
		else :
			pname, pcount, ptype, poffset = None, None, None, 0

		q = self.r_map[pident]

		match q :
			case Base() :
				m_lst[-1] = (pname, 1, f"{q.mtype}{q.msize}", poffset)
				yield m_lst
			case Typedef() :
				# print("TYPEDEF", pname, depth, max_depth, max_depth is None or depth <= max_depth)
				m_lst[-1] = (pname, 1, q.alias, poffset)
				yield from self._walk(q.type, m_lst, max_depth, depth+1)
			case Structure() :
				# print("STRUCT ", pname, depth, max_depth, max_depth is None or depth <= max_depth)
				if max_depth is None or depth <= max_depth :
					for m in self.r_map[pident].detail :
						yield from self._walk(m.oid, m_lst + [(m.name, 1, None, poffset + m.offset),], max_depth, follow_pointer, depth+1)
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
			except (AttributeError, KeyError) :
				print(f"ERROR::{func}::{child}")
				raise

	def _parse_base_type(self, die) :
		p = Base(
			die.attributes['DW_AT_name'].value.decode('utf8'),
			die_encoding_str[die.attributes['DW_AT_encoding'].value],
			die.attributes['DW_AT_byte_size'].value
		)
		self.r_map[die.offset] = p

	def _parse_typedef(self, die) :
		p = Typedef(
			die.attributes['DW_AT_type'].value,
			die.attributes['DW_AT_name'].value.decode('utf8')
		)
		self.r_map[die.offset] = p
		self.typedef_map[p.alias] = p.oid

	def _parse_array_type(self, die) :
		u_lst = list()
		for i, child in enumerate(die.iter_children()) :
			if 'DW_AT_upper_bound' in child.attributes :
				u_lst.append(child.attributes['DW_AT_upper_bound'].value + 1)
			else :
				print("error parsing array:", child)
		p = Array(
			die.attributes['DW_AT_type'].value,
			tuple(u_lst),
		)
		self.r_map[die.offset] = p

	def _parse_pointer_type(self, die) :
		p = Pointer(
			die.attributes['DW_AT_type'].value,
			die.attributes['DW_AT_byte_size'].value,
		)
		self.r_map[die.offset] = p

	def _parse_variable(self, die) :
		if 'DW_AT_name' in die.attributes :
			p = Variable(
				die.attributes['DW_AT_type'].value,
				die.attributes['DW_AT_name'].value.decode('utf8')
			)
			self.r_map[die.offset] = p
			self.variable_map[p.name] = p.oid

	def _parse_volatile_type(self, die) :
		if 'DW_AT_name' in die.attributes :
			p = Variable(
				die.attributes['DW_AT_type'].value,
				die.attributes['DW_AT_name'].value.decode('utf8')
			)
			self.r_map[die.offset] = p
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

	def _parse_enumeration_type(self, die) :
		pass

	def _parse_union_type(self, die) :
		pass

	def _parse_subprogram(self, die) :
		pass

	def _parse_subroutine_type(self, die) :
		pass

if __name__ == '__main__' :
	u = ElfParser(Path(sys.argv[1]))