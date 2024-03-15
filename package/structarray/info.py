#!/usr/bin/env python3

import re
import subprocess
import sys
import hashlib

from cc_pathlib import Path

struct_rec = re.compile(r'''struct\s*\{(?P<member>.*?)\}''', re.MULTILINE | re.DOTALL)
array_rec = re.compile(r'(?P<ctype>.*?)\s*\[(?P<array>\d+)\]')
addr_rec = re.compile(r'''\$[0-9]+ = (?P<addr>0x[0-9a-f]+)''')
member_rec = re.compile(r'''\s*(?P<ctype>.*?)\b\s*(?P<pointer>\*)?\s*\b(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)(\[(?P<array>\d+)\])?\s*;''')

def unp(s) :
	return s.replace('.*' , '->')

def split_cpm(item) :
	if '*' in item :
		left, null, right = item.partition('*')
		return [left.strip(), True, right.strip()]
	else :
		left, right = item.split()
		return [left.strip(), False, right.strip()]

class MetaParser() :

	debug = True
	version = 2

	def __init__(self, elf_pth, ctype_pth=None) :

		if self.debug :
			self.log_pth = Path("debug.log")
			self.log_pth.write_text('')
			
		self.elf_pth = elf_pth.resolve()

		self.addr = list()
		self.tree = dict()

		self._to_be_parsed_set = set()
		
		self.ctype_pth = (self.elf_pth.parent / "structarray_ctype.json") if ctype_pth is None else ctype_pth
		self.ctype_map = self.ctype_pth.load()

	def _gdb(self, * cmd_lst, chunk_size=1024) :

		cmd_lst = list(cmd_lst)
		stack = list()

		chunk = cmd_lst[:chunk_size]
		while chunk :
			# print(f'\t{len(cmd_lst)}')
			line = ['gdb', str(self.elf_pth), '-batch',]
			for cmd in chunk :
				line += ['-ex', cmd]

			ret = subprocess.run(line, stdout=subprocess.PIPE)
			stack.append(ret.stdout.decode(sys.stdout.encoding))

			cmd_lst = cmd_lst[chunk_size:]
			chunk = cmd_lst[:chunk_size]

		return ''.join(stack)

	def path_walk(self, pname, ctype=None, follow_pointers=False) :
		pass

	def walk(self, ctype=None, path_lst=None, follow_pointers=False, depth=0) :
		if ctype is None :
			ctype = self.var_type
		if ctype not in self.tree :
			raise ValueError(f"StructInfo.walk() unknown ctype: {ctype}")

		if path_lst is None :
			path_lst = list()
			
		if isinstance(self.tree[ctype], list) :
			for c, p, m in self.tree[ctype] :
				if depth == 0 :
					# print(c, p, m)
					pass
				if p :
					if follow_pointers :
						yield from self.walk(c, path_lst + [m + '*',], follow_pointers, depth+1)
					else :
						yield path_lst + [m, 'void*']
				else :
					yield from self.walk(c, path_lst + [m,], follow_pointers, depth+1)
		else :
			yield path_lst + [self.tree[ctype],]

	def get_addr(self, * path_lst, relative_to=0) :
		print(f">>> StructInfo.get_addr( {len(path_lst)} )", file=sys.stderr)

		line_lst = self._gdb(* [f'p/a &({unp(path)})' for path in path_lst]).splitlines()
		addr_lst = list()
		for line in line_lst :
			if 'no member named' in line :
				print(path_lst)
				raise
			addr_res = addr_rec.search(line)
			addr_lst.append(int(addr_res.group('addr'), 16) - relative_to)
		return addr_lst

	def get_tree(self, * ctype_lst) :
		print(f">>> StructInfo.get_tree( {len(ctype_lst)} )", file=sys.stderr)

		new_set = set()

		line = self._gdb(* [f'ptype {ctype}' for ctype in ctype_lst])
		ptype_lst = [item.strip() for item in line.split('type =') if item.strip()]

		if self.debug :
			with self.log_pth.open('at') as fid :
				fid.write(">"*8 + '\n')
				fid.write('\n'.join(ctype_lst) + '\n')
				fid.write("-"*8 + '\n')
				fid.write(line)
				# fid.write("-"*8 + '\n')
				# fid.write('\n'.join(ptype_lst) + '\n')
				fid.write("<"*8 + '\n')

		for ctype, ptype in zip(ctype_lst, ptype_lst) :

			if self.debug :
				with self.log_pth.open('at') as fid :
					fid.write(">>> " + ctype + ' := ' + repr(ptype) +'\n')

			if (struct_res := struct_rec.match(ptype)) is not None :
				struct_lst = list()
				for member_res in member_rec.finditer(struct_res.group('member')) :
					if member_res.group('array') is None :
						if member_res.group('ctype') not in ["void",] :
							struct_lst.append([member_res.group('ctype').strip(), (member_res.group('pointer') is not None), member_res.group('name')])
					else :
						for i in range(int(member_res.group('array'))) :
							struct_lst.append([member_res.group('ctype').strip(), (member_res.group('pointer') is not None), member_res.group('name') + f'[{i}]'])
				self.tree[ctype] = struct_lst
				new_set |= set(c for c, p, m in struct_lst)
			elif (array_res := array_rec.match(ptype)) is not None :
				array_lst = list()
				for i in range(int(array_res.group('array'))) :
					array_lst.append([array_res.group('ctype').strip(), False, f'[{i}]'])
				self.tree[ctype] = array_lst
				new_set.add(array_res.group('ctype').strip())
			else :
				if ptype not in self.ctype_map :
					raise ValueError(f"unknown ctype: {ptype}")
				self.tree[ctype] = ptype

		return new_set

	def get_sizeof(self, ctype) :
		line = self._gdb(f'print sizeof({ctype})')
		left, sep, right = line.partition('=')
		return int(right.strip())

	def parse(self, var_name) :

		line = self._gdb(f'whatis {var_name}')
		if line.startswith('type =') :
			var_type = line.partition('=')[-1].strip()
		else :
			raise ValueError(f"this var is not known: {var_name}")

		var_size = self.get_sizeof(var_type)

		origin = self.get_addr(var_name).pop()

		self.parse_tree(var_name, var_type)
		self.parse_addr(var_name, var_type, origin)

		self.var_name = var_name
		self.var_type = var_type
		self.var_size = var_size

		print(f"sizeof({var_type}) = {var_size}")

		return var_size

	def save_absolute(self, pth) :
		pth.with_suffix('.tree.json').save(self.tree, verbose=True)
		pth.with_suffix('.tsv').save([[self.var_type, self.var_size],] + self.addr)

	def save_relative(self, pth) :
		pth.with_suffix('.json').save(self.tree)

		s_lst = list()
		prev_addr = 0
		for name, ctype, curr_addr in self.addr :
			if s_lst :
				u = curr_addr - prev_addr - int(s_lst[-1][1][1:])
				if u != 0 :
					s_lst[-1].append(u)
			s_lst.append([name, ctype,])
			prev_addr = curr_addr

		u = self.var_size - prev_addr - int(s_lst[-1][1][1:])
		if u != 0 :
			s_lst[-1].append(u)

		pth.with_suffix('.tsv').save([[self.var_type, self.var_size],] + s_lst)

	def print(self) :
		for line in ([[self.var_type, self.var_size],] + self.addr) :
			print('\t'.join([str(i) for i in line]))

	def parse_tree(self, vname, ctype) :
		# fill self.tree with the detail of all types found below the ctype given
		self.tree = dict()

		todo_set = set()
		todo_set = ( todo_set | self.get_tree(ctype) ) - self.tree.keys()
		while todo_set :
			todo_set = ( todo_set | self.get_tree(* sorted(todo_set)) ) - self.tree.keys()

	def parse_addr(self, vname, ctype, origin=0) :

		line_lst = list( self.walk(ctype) )
		name_lst = [ unp('.'.join(line[:-1])) for line in line_lst ]
		path_lst = [ unp(f'{vname}.' + '.'.join(line[:-1])).replace('.[', '[') for line in line_lst ]
		type_lst = [ line[-1] for line in line_lst ]
		addr_lst = self.get_addr(* path_lst, relative_to=origin)

		self.addr = [
			[ name.replace('.[', '['), self.ctype_map[ptype], addr ]
			for name, ptype, addr in zip(name_lst, type_lst, addr_lst)
		]
