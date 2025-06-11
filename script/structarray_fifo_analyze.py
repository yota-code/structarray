#!/usr/bin/env python3

import array
import struct
from structarray.meta import MetaReb

import argparse
import numpy as np
import matplotlib.pyplot as plt

from cc_pathlib import Path

parser = argparse.ArgumentParser(description='Convert a meta (*.sam.tsv) and data (*.sad.bin) file into a tsv file')

parser.add_argument('data', metavar='DATA', type=Path, help='the data (*.reb) file')
parser.add_argument('meta', metavar='META', type=Path, nargs='?', default=None, help='the meta (*.tsv) file')

p = parser.parse_args()

data_pth = (p.data).resolve()

if p.meta is None :
	meta_pth = (p.data.parent / "mapping.tsv")
else :
	meta_pth = (p.meta).resolve()

meta = MetaReb().load(meta_pth)


#_L204_MfcAfcs.nb_labels	N4	25856
# _L204_MfcAfcs.labels._F0	N4	25864
# _L204_MfcAfcs.labels._F1	N4	25868
# _L204_MfcAfcs.labels._F2	N4	25872
# _L204_MfcAfcs.labels._F3	N4	25876
# _L204_MfcAfcs.labels._F4	N4	25880

N_offset = meta["_L204_MfcAfcs.nb_labels"][1]
F_offset = meta["_L204_MfcAfcs.labels._F0"][1]

with data_pth.open('rb') as fid :
	while block := fid.read(meta.sizeof) :
		nb_label = struct.unpack_from('I', block, N_offset)[0]
		m = block[F_offset:4*nb_label + F_offset]
		label_array = array.array('I', m)
		# print(nb_label, ' '.join(f"{i:08X}" for i in label_array))
		print(nb_label, "::",  ' '.join(f"{i&0xFF:03o}" for i in label_array))