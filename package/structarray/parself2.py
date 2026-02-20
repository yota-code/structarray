#!/usr/bin/env python3

"""
le moyen le plus rapide de récupérer le .debug_info:
readelf -wi *.o
"""

import enum
import collections
import sys
import time

from cc_pathlib import Path

from elftools.elf.elffile import ELFFile

die_encoding_str = 'xyNwRZZNN'

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

	@property
	def letter(self) :
		return "P"

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


class PointerDo(enum.Enum):
    HIDE = -1
    DISPLAY = 0
    FOLLOW = 1

class ElfParser() :

	debug = False

	def __init__(self, elf_pth) :
		self._time_lst = [time.time(),]

		# self.u = TypeTree()
		
		self.r_map = collections.defaultdict(dict)
		self.s_map = dict() # liste des symboles de haut niveau
		self.p_map = dict() # reverse type definitions, get parents 

		self.typedef_map = dict()
		self.variable_map = dict()
		self.base_map = dict()

		for top in self.load(elf_pth) :
			self.parse(top)
			self.chrono(f"parse(\x1b[33m{top.attributes['DW_AT_name'].value.decode('utf8')}\x1b[0m)")

		if self.debug :
			Path("r_map.json").save(self.r_map, verbose=True)
			Path("s_map.json").save(self.s_map, verbose=True)
			Path("p_map.json").save(self.p_map, verbose=True)
			Path("typedef_map.json").save(self.typedef_map, verbose=True)
			Path("variable_map.json").save(self.variable_map, verbose=True)
			Path("base_map.json").save(self.base_map, verbose=True)

		# w_lst = list()
		# for i, m_lst in enumerate(self.walk('_C_MfcAfcs')) :
		# 	w_lst.append(self.expand(m_lst))
		# 	if i > 100 :
		# 		break
		# Path("walk.txt").write_text('\n'.join(w_lst))


		# w_lst = list()
		# for i, m_lst in enumerate(self.walk('_C_MfcAfcs')) :
		# 	w_lst.append(self.expand_struct(m_lst))
		# Path("walk.tsv").save(w_lst)

	def get_meta(self, name, model=None) :

		# model name, helps to cleanup scade structures from generic suffixes
		def cleanup(s) :
			if model is not None and ( s.startswith('_L') or s.startswith('_M') ) :
				return s.replace('_' + model, '')
			return s
	
		oid = self.get_root(name)

		# def as_array(shape) :
		# 	return ''.join(f'[{s}]' for s in shape) if isinstance(shape, tuple) else ''

		from structarray.rebin.meta import MetaRebin

		u = MetaRebin(self.r_map[oid].alias, self.to_base(self.r_map[oid]).sizeof)

		for m_lst in self.walk(oid) :
			p_lst = [cleanup(obj.name) for oid, t_lst, obj, offset in m_lst if isinstance(obj, Member)]
			key = '.'.join(p_lst)
			u[key] = (f"{m_lst[-1][2].letter}{m_lst[-1][2].sizeof}", m_lst[-1][3])

		return u

	def resolve(self, name_or_oid) :
		if isinstance(name_or_oid, str) :
			name = name_or_oid
			if name in self.variable_map :
				oid = self.variable_map[name]
			elif name in self.typedef_map :
				oid = self.typedef_map[name]
			else :
				raise ValueError
		elif isinstance(name_or_oid, int) :
			oid = name_or_oid

		# if oid in self.s_map :
		# 	oid = self.s_map[oid]

		# while oid in self.r_map and isinstance(self.r_map[oid], (Typedef, Pointer)) :
		# 	oid = self.r_map[oid].oid

		print(f">>> resolve({name_or_oid}) -> {oid}/{self.r_map[oid]}")
		
		return oid

	def sizeof(self, name_or_oid) :
		oid = self.resolve(name_or_oid)
		sizeof = self.r_map[oid].size

	def to_base(self, q) :
		while isinstance(q, Typedef) :
			q = self.r_map[q.oid]
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

	def iter_by_offset(self, name) :
		# take a m_lst, return the path to the object
		for m_lst in self.walk(name, follow_pointer=False) :
			s_lst = list()
			for oid, t_lst, obj, offset in m_lst :
				if isinstance(obj, Member) :
					s_lst.append(obj.name)

			oid, t_lst, obj, offset = m_lst[-1]
			if isinstance(obj, Base) :
				yield s_lst, f"{obj.letter}{obj.sizeof}", offset
			else :
				yield s_lst, f"P{obj.sizeof}", offset

	def get_root(self, name) :
		if name in self.variable_map :
			oid = self.variable_map[name]
		elif name in self.typedef_map :
			oid = self.typedef_map[name]
		else :
			if isinstance(name, int) :
				oid = name
			else :
				raise ValueError(f"Can't find a reference to {name} either as a variable or as a type")

		obj = self.r_map[oid]

		assert isinstance(obj, Typedef)

		return oid

	def walk(self, oid, max_depth=None, pointer=PointerDo.DISPLAY) :
		# TODO: au lieu de mettre un simple oid, on peut mettre un o_lst qui cumule la liste des oid traversés... ou alors juste le un champ oid type
		obj = self.r_map[oid]
		yield from self._walk([(oid, list(), obj, 0),], max_depth, pointer)

	def _walk(self, m_lst, max_depth, pointer, depth=0) :
		
		# print("\t" + "-" * (depth+1) + "> " + f"{self.expand(m_lst)}", max_depth, depth)
		# with Path("walk.raw").open('at') as fid :
		#  	fid.write(str(m_lst) + '\n')

		oid, t_lst, obj, offset = m_lst[-1]

		match obj :
			case Base() :
				yield m_lst
			case Typedef() :
				yield from self._walk(m_lst + [(obj.oid, t_lst + [oid,], self.r_map[obj.oid], offset),], max_depth, pointer, depth)
			case Member() :
				yield from self._walk(m_lst + [(obj.oid, list(), self.r_map[obj.oid], offset),], max_depth, pointer, depth+1)
			case Structure() :
				if max_depth is None or depth <= max_depth :
					for i, sub in enumerate(self.r_map[oid].detail) :
						yield from self._walk(m_lst + [(f"{i}", t_lst, sub, offset + sub.offset),], max_depth, pointer, depth)
				else :
					yield m_lst
			case Pointer() :
				match pointer :
					case PointerDo.HIDE :
						return
					case PointerDo.DISPLAY :
						yield m_lst
					case PointerDo.FOLLOW :
						yield from self._walk(m_lst + [(obj.oid, list(), self.r_map[obj.oid], offset),], max_depth, pointer, depth)

	# def walk_smart(self, name=None, max_depth=None, follow_pointer=False) :
	# 	pident = self.get_oid(self.default_name if name is None else name)

	# 	self.sizeof = self.r_map[pident].size

	# 	yield from self._walk_smart(pident, list(), max_depth, follow_pointer)

	# def _walk_smart(self, pident, m_lst, max_depth, follow_pointer, depth=0) : 
	# 	if m_lst :
	# 		pname, pcount, ptype, poffset = m_lst[-1]
	# 	else :
	# 		pname, pcount, ptype, poffset = None, None, None, 0

	# 	q = self.r_map[pident]

	# 	match q :
	# 		case Base() :
	# 			m_lst[-1] = (pname, 1, f"{q.mtype}{q.msize}", poffset)
	# 			yield m_lst
	# 		case Typedef() :
	# 			# print("TYPEDEF", pname, depth, max_depth, max_depth is None or depth <= max_depth)
	# 			m_lst[-1] = (pname, 1, q.alias, poffset)
	# 			yield from self._walk(q.type, m_lst, max_depth, depth+1)
	# 		case Structure() :
	# 			# print("STRUCT ", pname, depth, max_depth, max_depth is None or depth <= max_depth)
	# 			if max_depth is None or depth <= max_depth :
	# 				for m in self.r_map[pident].detail :
	# 					yield from self._walk(m.oid, m_lst + [(m.name, 1, None, poffset + m.offset),], max_depth, follow_pointer, depth+1)
	# 			else :
	# 				yield m_lst
	# 		case Pointer() :
	# 			# self.r_map[q.type].name
	# 			if follow_pointer :
	# 				while pident in self.r_map and isinstance(self.r_map[pident], (Typedef, Pointer)) :
	# 					# print(f"R_MAP {pident} -> {self.r_map[pident][0]}")
	# 					pident = self.r_map[pident].type
	# 				for m in self.r_map[pident].detail :
	# 					yield from self._walk(m.type, m_lst + [(m.name + '*', 1, None, poffset + m.offset),], max_depth, follow_pointer, depth+1)
	# 			else :
	# 				m_lst[-1] = (pname, 0, f"P{q.size}", poffset)
	# 				yield m_lst
	# 		case Array() :
	# 			m_lst[-1] = (pname, q.shape, self.to_base(self.r_map[q.type]).mtype, poffset)
	# 			yield m_lst
	# 		case _ :
	# 			raise ValueError(m_lst, q)

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
				# try :
				# 	self.p_map[self.r_map[child.offset].oid] = child.offset
				# except :
				# 	pass
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
		self.typedef_map[p.alias] = die.offset

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

	def _parse_const_type(self, die) :
		pass


if __name__ == '__main__' :
	u = ElfParser(Path(sys.argv[1]))
	p = 'unitest_context'
	# p = '_C_MfcAfcs'
	# p = 704033

	Path("walk_by_offset.tsv").save([m + [p, o]  for m, p, o in u.iter_by_offset(p)])

	w_lst = [m_lst for m_lst in u.walk(p)][:100]
	Path("walk_raw.tsv").save(w_lst)
