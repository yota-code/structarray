#!/usr/bin/env python3

import re
import subprocess
import sys
import hashlib

from cc_pathlib import Path

struct_rec = re.compile(r'''struct\s*\{(?P<member>.*?)\}''', re.MULTILINE | re.DOTALL)
addr_rec = re.compile(r'''\$[0-9]+ = 0x[0-9a-f]+ <.*?(\+(?P<addr>[0-9]+))?>''')

def unp(s) :
	return s.replace('.*' , '->')

class StructInfo() :
	def __init__(self, elf_pth) :

		self.elf_pth = elf_pth.resolve()

		self.info = dict()
		self.addr = dict()
		self._to_be_parsed_set = set()
		self.ctype_map = (self.elf_pth.parent / "structarray_type.json").load()

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

	def walk(self, ctype, path_lst=None, depth=0) :
		if path_lst is None :
			path_lst = list()
		if isinstance(self.info[ctype], list) :
			for c, p, m in self.info[ctype] :
				if depth == 0 :
					# print(c, p, m)
					pass
				if p :
					yield path_lst + ['*' + m, 'void*']
				else :
					yield from self.walk(c, path_lst + [m,], depth+1)
		else :
			yield  path_lst + [self.info[ctype],]

	def get_addr(self, * path_lst) :
		print(f" >> StructInfo.get_addr( {len(path_lst)} )")
		line_lst = self._gdb(* [f'p/a &({unp(path)})' for path in path_lst]).splitlines()
		addr_lst = list()
		for line in line_lst :
			addr_res = addr_rec.match(line)
			addr_lst.append(int(addr_res.group('addr')) if addr_res.group('addr') is not None else 0)
		return addr_lst

	def get_type(self, * ctype_lst) :
		print(f" >> StructInfo.get_type( {len(ctype_lst)} )")
		def split_cpm(item) :
			if '*' in item :
				left, null, right = item.partition('*')
				return [left.strip(), True, right.strip()]
			else :
				left, right = item.split()
				return [left.strip(), False, right.strip()]

		line = self._gdb(* [f'ptype {ctype}' for ctype in ctype_lst])
		ptype_lst = [item.strip() for item in line.split('type =') if item.strip()]
		
		for ctype, ptype in zip(ctype_lst, ptype_lst) :
			struct_res = struct_rec.match(ptype)
			if struct_res is not None :
				member = struct_res.group('member')
				struct = [split_cpm(item.strip().strip(';').strip()) for item in member.splitlines()[1:]]
				self.info[ctype] = struct
				self._to_be_parsed_set |= set(c for c, p, m in struct)
			else :
				self.info[ctype] = ptype

	def get_sizeof(self, ctype) :
		line = self._gdb(f'print sizeof({ctype})')
		left, sep, right = line.partition('=')
		return int(right.strip())

	def parse(self, ctype, varname) :
		ssize = self.get_sizeof(ctype)

		stack = list()
		stack.append([ctype, ssize,])

		self.get_type(ctype)
		to_be_parsed_set = self._to_be_parsed_set - self.info.keys()
		while to_be_parsed_set :
			# print(', '.join(to_be_parsed_set))
			self.get_type(* to_be_parsed_set)
			to_be_parsed_set = self._to_be_parsed_set - self.info.keys()

		path_lst = list()
		path_map = dict()
		for line in self.walk(ctype) :
			var = unp(f'{varname}.' + '.'.join(line[:-1]))
			typ = line[-1]
			path_map[var] = typ
			path_lst.append(var)


		addr_lst = self.get_addr(* path_lst)

		j = len(varname)
		for var, addr in zip(path_lst, addr_lst) :
			stack.append([var.replace('->', '.')[j+1:], self.ctype_map[path_map[var]], addr])

		src_pth = self.elf_pth.parent / f"{ctype}.tsv"
		src_pth.save(stack)

		hash = hashlib.blake2b(src_pth.read_bytes()).hexdigest()[:12]
		dst_pth = self.elf_pth.parent / f"{ctype}.{hash}.sam.tsv"

		src_pth.rename(dst_pth)

		print('\n---', dst_pth)


