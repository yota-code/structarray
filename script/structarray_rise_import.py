#!/usr/bin/env python3

import hashlib
import sys

import structarray

from cc_pathlib import Path

cwd = Path(sys.argv[1])

rise_data_pth = cwd / "Data"
rise_meta_pth = cwd / "libModelUPMV.txt"

structarray_meta_pth = cwd / "meta.tsv"

ctyp_rise_map = {
	'int' : "Z4",
	'float' : "R4",
}

ctype_size_map = {
	'Z4' : 4,
	'R4' : 4,
}

def rise_to_meta(rise_pth) :
	stack = list()
	for line in rise_pth.read_text().splitlines() :
		if not line.startswith('upmv_C') :
			continue
		name, addr, ctyp = line.split()
		if ctyp == 'void' :
			print(name, ctyp)
			continue
		name = name[7:]
		addr = int(addr)
		ctyp = ctyp_rise_map[ctyp]
		stack.append([name, ctyp, addr])
	
	total_size = addr + ctype_size_map[ctyp]
	
	stack = [ ['upmv_C', total_size], ] + stack
	
	return stack
	
if __name__ == '__main__' :
	
	stack = rise_to_meta( rise_meta_pth )

	structarray_meta_pth.save(stack)
	h = hashlib.blake2b(structarray_meta_pth.read_bytes(), digest_size=10).hexdigest()
	
	structarray_data_pth = cwd / f"data.{h}.bin"
	if not structarray_data_pth.is_file() :
		rise_data_pth.hardlink_to(structarray_data_pth)
	
	u = structarray.StructArray(structarray_meta_pth, structarray_data_pth)
	u.to_tsv(cwd / "data.tsv")
	
	current_pth = (cwd / '../_last_').resolve()
	if not current_pth.is_symlink() :
		current_pth.symlink_to(cwd)