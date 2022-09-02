#☺!/usr/bin/env python3

import sys

def block_compress(src_pth, block_size) :
	prev = None
	dst_pth = src_pth.with_suffix('.rez')
	with dst_pth.open('wb') as fid_w :
		with src_pth.open('rb') as fid_r :
			while True :
				block = fid_r.read(block_size)
				if len(block) == 0 :
					break
				if len(block) != block_size :
					raise ValueError
				if prev is not None :
					block = bytes(x ^ y for x, y in zip(prev, block))
				fid_w.write(block)
				prev = block

if __name__ == '__main__' :
	from pathlib import Path

	block_compress(Path(sys.argv[1]), 368608)