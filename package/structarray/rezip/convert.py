#!/usr/bin/env python3

import collections

import numpy as np

from cc_pathlib import Path

import structarray.rebin.data

def to_rez(data, meta=None) :
	""" 
	Converti un fichier .reb en fichier .rez
	"""

	p = "\x1b[A\x1b[K"

	match data :
		case Path() | str() :
			data = structarray.rebin.data.DataRebin(Path(data).resolve(strict=True), meta)
		case structarray.data.DataHandler() :
			data = data

	meta = data.meta

	import brotli
	import h5py

	archive_pth = data.data_pth.with_suffix('.rez')
	if archive_pth.is_file() :
		archive_pth.unlink()

	h5py_opt = {
		'compression' : "gzip",
		'compression_opts' : 9,
		'shuffle' : True,
		'fletcher32' : True,
	}

	v_lst = list(meta._m)
	e_map = dict()
	for c in ['R8', 'R4', 'Z4', 'Z2', 'Z1', 'N4', 'N2', 'N1'] :
		i_lst = [i for i, v in enumerate(v_lst) if meta[v][0] == c]
		print(f"{c} {0:7d} / {len(i_lst)}")
		if i_lst :
			e_map[c] = dict() # ctype -> name -> position
			s = collections.defaultdict(set) # hash -> position set
			m = list()
			for n, i in enumerate(i_lst) :
				v = v_lst[i] # full name of the variable
				d = data[v] # full data line extracted
				if d[0] == d[-1] and (d[0] == d).all() :
					e_map[c][i] = ('=', d[0])
					print(f"{p}{c} {n+1:7d} / {len(i_lst)} # {d[0]} = {v_lst[i]}")
				else :
					h = hash(d.tobytes()) # hash of the line
					j = len(m)
					if h not in s :
						s[h].add(j)
						m.append(d)
					else :
						for j in s[h] :
							if d.tobytes() == m[j].tobytes() :
								break
						else :
							s[h].add(j)
							m.append(d)
					e_map[c][i] = ('@', j)
					print(f"{p}{c} {n+1:7d} / {len(i_lst)} # {j} @ {v_lst[i]}")

			Path(f"s_map.{c}.json").save(s)
			
			if m :
				with h5py.File(archive_pth, 'a', libver="latest") as obj :
					w = np.vstack(m)
					print(f"{p}{c} {len(i_lst):7d} / {len(i_lst)} => {w.shape[0]} rows")
					obj.create_dataset('/' + c, data=w, ** h5py_opt)

	f_lst = [str(data.vector_len),] # on doit garder vector_len dans les méta données parce qu'il se peut que TOUS les vecteurs soient constants

	compact = meta._proc_name_compact()
	next(compact)

	for i, v in enumerate(v_lst) :
		r = compact.send(v)
		mtype = meta[v][0]
		if mtype in e_map :
			z, j = e_map[mtype][i]
		else :
			z, j = '', ''
		f_lst.append(f'{r}\t{mtype}{z}{j}')

	Path(archive_pth.with_suffix('.mez')).write_text('\n'.join(f_lst))

	with h5py.File(archive_pth, 'a', libver="latest") as obj :
		meta_zip = brotli.compress('\n'.join(f_lst).encode('ascii'), mode=brotli.MODE_TEXT)
		obj.attrs['%meta%'] = np.void(meta_zip) # https://docs.h5py.org/en/stable/strings.html

	data_size = data.data_pth.stat().st_size
	meta_size = meta.meta_pth.stat().st_size
	archive_size = archive_pth.stat().st_size

	print(f"\noriginal: {data_size + meta_size:15d} bytes ({meta_size:8d} meta)\n archive: {archive_size:15d} bytes ({len(meta_zip):8d} meta)\n => archive takes {100.0 * archive_size / (data_size + meta_size):0.3}% of original")

	return archive_pth
