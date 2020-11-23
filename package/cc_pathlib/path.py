#!/usr/bin/env python3

import bz2
import datetime
import lzma
import gzip
import pathlib
import subprocess
import socket
import sys

try :
	import brotli
except :
	pass

import cc_pathlib.filter.cc_json
import cc_pathlib.filter.cc_pickle
import cc_pathlib.filter.tsv

class Path(type(pathlib.Path())) :

	_available_archive = ['.gz', '.xz', '.br', '.lz']
	_available_filter = ['.tsv', '.json', '.txt', '.bin', '.pickle']
	_binary_format = ['.pickle']

	_umask_dir_map = {
		'private' : 0o0700,
		'shared' : 0o2770,
		'public' : 0o2777,
	}

	def config(self) :
		if self.suffix == ".json" :
			return cc_pathlib.filter.cc_json._JSON_config_CONTEXT(self)
		else :
			raise NotImplementedError

	def safe_config(self, lock) :
		if self.suffix == ".json" :
			with lock :
				return cc_pathlib.filter.cc_json._JSON_config_CONTEXT(self)
		else :
			raise NotImplementedError

	def __iter__(self) :
		return (x for x in self.iterdir())

	@property
	def or_archive(self) :
		# if an archived version of the file exists, switch to it
		if not self.is_file() :
			for suffixe in ['.br', '.lz', '.gz'] :
				pth = self.parent / (self.name + suffixe)
				if pth.is_file() :
					return pth
		return self

	def archive(self, dst_dir=None, timestamp=False, delete_original=False) :
		dst_name = self.name

		if timestamp is True :
			dst_name += '.{0:%Y%m%d_%H%M%S}'.format(datetime.datetime.now())
		elif isinstance(timestamp, str) :
			dst_name += '.' + timestamp

		if dst_dir is None :
			dst_dir = self.parent

		if self.is_dir() :
			tar_pth = dst_dir / (dst_name + '.tar')
			cmd = ['tar', '--create', '--file', tar_pth, self.name]
			subprocess.run([str(i) for i in cmd], cwd=self.parent)
			cmd = ['lzip', '--best', tar_pth]
			subprocess.run([str(i) for i in cmd], cwd=self.parent)
			if delete_original :
				self.delete()
		elif self.is_file() :
			cmd = ['lzip', '--best', self]
			subprocess.run([str(i) for i in cmd])
			self.rename(dst_dir / dst_name)
			if delete_original :
				self.unlink()
		else :
			raise ValueError("can not be archived: {0}".format(self))

	def unarchive(self) :
		cmd = ['tar', '--extract', '--file', self.name]
		subprocess.run([str(i) for i in cmd], cwd=self.parent)

	def delete(self, content_only=False) :
		if not self.is_dir() :
			return
		for sub in self.iterdir() :
			if sub.is_dir() :
				sub.delete()
			else :
				sub.unlink()
		if not content_only :
			self.rmdir() # if you just want to delete dir content, remove this line

	def make_dirs(self, umask='shared') :
		""" create all dirs, equivalent to mkdir(parents=True) but also set permissions """
		if not self.is_dir() :
			self.make_parents(umask)

			self.mkdir()
			self.chmod(self._umask_dir_map[umask])

	def make_parents(self, umask='shared') :
		""" create all parent dirs of a given file to be """
		for p in reversed(self.parents) :
			if not p.is_dir() :
				p.mkdir()
				p.chmod(self._umask_dir_map[umask])

	def _load_archive(self, fmt, encoding) :
		""" open the compressed file, return the content """
		if fmt == '.gz' :
			z_content = self.read_bytes()
			b_content = gzip.decompress(z_content)
		elif fmt == '.xz' :
			z_content = self.read_bytes()
			b_content = lzma.decompress(z_content)
		elif fmt == '.br' :
			z_content = self.read_bytes()
			b_content = brotli.decompress(z_content)
		elif fmt == '.lz' :
			cmd = ['lzip', '--decompress', '--stdout', self]
			ret = subprocess.run([str(i) for i in cmd], stdout=subprocess.PIPE)
			b_content = ret.stdout
		else :
			# if fmt is None, load the file content as this
			b_content = self.read_bytes()

		return b_content if encoding is None else b_content.decode(encoding)

	def _load_filter(self, data, fmt, opt=None) :
		if fmt == '.tsv' :
			return cc_pathlib.filter.tsv.tsv_from_str(data)
		elif fmt == '.json' :
			return cc_pathlib.filter.cc_json.json_from_str(data)
		elif fmt == '.pickle' :
			return cc_pathlib.filter.cc_pickle.pickle_from_str(data)
		else :
			return data

	def load(self, encoding='utf-8') :
		s_lst = self.suffixes
		fmt = None
		if s_lst and s_lst[-1] in self._available_archive :
			fmt = s_lst.pop()
		if s_lst[-1] in self._binary_format :
			encoding = None
		data = self._load_archive(fmt, encoding)
		if s_lst and s_lst[-1] in self._available_filter :
			data = self._load_filter(data, s_lst[-1])
		return data

	def save(self, data, encoding='utf-8', archive_opt=None, filter_opt=None, make_dirs='shared') :
		s_lst = self.suffixes

		self.make_parents(make_dirs)

		fmt = None
		if s_lst and s_lst[-1] in self._available_archive :
			fmt = s_lst.pop()
		if s_lst and s_lst[-1] in self._available_filter :
			data = self._save_filter(data, s_lst.pop(), filter_opt)
		self._save_archive(data, fmt, encoding)

	def _save_archive(self, data, fmt, encoding='utf-8', opt=None) :
		# print("Path._save_archive({0})".format(fmt))
		b_data = data if isinstance(data, bytes) else data.encode(encoding)
		if fmt == '.gz' :
			z_data = gzip.compress(b_data, compresslevel=9)
			self.write_bytes(z_data)
		elif fmt == '.xz' :
			z_data = lzma.compress(b_data, preset=9 | lzma.PRESET_EXTREME)
			self.write_bytes(z_data)
		elif fmt == '.br' :
			z_data = brotli.compress(b_data)
			self.write_bytes(z_data)
		elif fmt == '.lz' :
			cmd = ['lzip', '--best', '--force', '--output', self.with_suffix('')]
			ret = subprocess.run([str(i) for i in cmd], input=b_data)
		else :
			self.write_bytes(b_data)

	def _save_filter(self, data, fmt, opt=None) :
		# print("Path._save_filter({0}, {1}, {2})".format(type(data), fmt, opt))
		if opt is None :
			opt = dict()

		if fmt == '.tsv' :
			return cc_pathlib.filter.tsv.tsv_to_str(data, ** opt)
		elif fmt == '.json' :
			return cc_pathlib.filter.cc_json.json_to_str(data, ** opt)
		elif fmt == '.pickle' :
			return cc_pathlib.filter.cc_pickle.pickle_to_str(data, ** opt)
		else :
			return data

	def hardlink_to(self, target) :
		if self.is_file() :
			os.link(str(self), str(target))
		else :
			raise ValueError("hardlink source must be a file")

	@property
	def fname(self) :
		s = ''.join(self.suffixes)
		return self.name[:-len(s)]

	def run(self, * cmd_lst, timeout=None, blocking=True, bg_task=False, quiet=False) :

		cwd = (self).resolve()

		cmd_line = list()
		for cmd in cmd_lst :
			if isinstance(cmd, dict) :
				for k, v in cmd.items() :
					cmd_line.append('--' + str(k))
					cmd_line.append(str(v))
			else :
				cmd_line.append(str(cmd))

		cmd_header = '\x1b[44m{0} {1}{2} $\x1b[0m '.format(
			socket.gethostname(),
			"{0} ".format(Path(* cwd.parts[-3:])),
			datetime.datetime.now().strftime('%H:%M:%S')
		)

		if not quiet :
			try :
				cwd_rel = cwd.relative_to(self)
			except :
				cwd_rel = cwd
			print(cmd_header + ' '.join(str(i) for i in cmd_line))

		if bg_task :
			subprocess.Popen(cmd_line, cwd=(str(cwd) if cwd is not None else cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		else :
			ret = subprocess.run(cmd_line, cwd=(str(cwd) if cwd is not None else cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
			if blocking and ret.returncode != 0 :
				if not quiet :
					print('\n' + ' '.join(ret.args) + '\n' + ret.stderr.decode(sys.stderr.encoding) + '\n' + '-' * 32)
				ret.check_returncode()
			return ret

