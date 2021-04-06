#!/usr/bin/env python3

import re
import subprocess
import sys
import hashlib

from cc_pathlib import Path

struct_rec = re.compile(r'''struct\s*\{(?P<member>.*?)\}''', re.MULTILINE | re.DOTALL)
addr_rec = re.compile(r'''\$[0-9]+ = (?P<addr>0x[0-9a-f]+)''')

def unp(s) :
	return s.replace('.*' , '->')

def split_cpm(item) :
	if '*' in item :
		left, null, right = item.partition('*')
		return [left.strip(), True, right.strip()]
	else :
		left, right = item.split()
		return [left.strip(), False, right.strip()]

class StructInfo() :
	def __init__(self, elf_pth) :

		self.elf_pth = elf_pth.resolve()

		self.addr = list()
		self.tree = dict()

		self._to_be_parsed_set = set()
		
		self.ctype_map = (self.elf_pth.parent / "structarray_ctype.json").load()

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
		print(f">>> StructInfo.get_addr( {len(path_lst)} )")
		line_lst = self._gdb(* [f'p/a &({unp(path)})' for path in path_lst]).splitlines()
		addr_lst = list()
		for line in line_lst :
			addr_res = addr_rec.search(line)
			addr_lst.append(int(addr_res.group('addr'), 16) - relative_to)
		return addr_lst

	def get_tree(self, * ctype_lst) :
		print(f">>> StructInfo.get_tree( {len(ctype_lst)} )")

		new_set = set()

		line = self._gdb(* [f'ptype {ctype}' for ctype in ctype_lst])
		ptype_lst = [item.strip() for item in line.split('type =') if item.strip()]
		
		for ctype, ptype in zip(ctype_lst, ptype_lst) :
			struct_res = struct_rec.match(ptype)
			if struct_res is not None :
				member = struct_res.group('member')
				struct = [split_cpm(item.strip().strip(';').strip()) for item in member.splitlines()[1:]]
				self.tree[ctype] = struct

				new_set |= set(c for c, p, m in struct)
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

	def save(self, dst_dir) :
		(dst_dir / "structarray_info.json").save(self.tree)

		src_pth = dst_dir / f"{self.var_type}.sam.tsv"

		# tambouille pour avec le hash dans le nom du fichier, môche

		try :
			src_pth.unlink()
		except FileNotFoundError :
			pass

		src_pth.save([[self.var_type, self.var_size],] + self.addr)
		h = hashlib.blake2b(src_pth.read_bytes()).hexdigest()[:12]

		dst_pth = dst_dir / f"{self.var_type}.{h}.sam.tsv"
		try :
			dst_pth.unlink()
		except FileNotFoundError :
			pass

		src_pth.rename(dst_pth)
			
		try :
			src_pth.unlink()
		except FileNotFoundError :
			pass
		src_pth.symlink_to(dst_pth)

		ctx_pth = dst_dir / f"context.tsv"
		try :
			ctx_pth.unlink()
		except FileNotFoundError :
			pass
		ctx_pth.hardlink_to(dst_pth)

		print('\n---', dst_pth)
		print('---', src_pth)


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
		path_lst = [ unp(f'{vname}.' + '.'.join(line[:-1])) for line in line_lst ]
		type_lst = [ line[-1] for line in line_lst ]
		addr_lst = self.get_addr(* path_lst, relative_to=origin)

		self.addr = [
			[ name, self.ctype_map[ptype], addr ]
			for name, ptype, addr in zip(name_lst, type_lst, addr_lst)
		]
