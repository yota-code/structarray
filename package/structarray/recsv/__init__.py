#!/usr/bin/env python3

import ast
import collections
import csv
import datetime

from pathlib import Path

import numpy as np

from structarray.data import DataGeneric
from structarray.meta import MetaGeneric

tf_map = {'true': True, 'false': False}
def eval(s) :
	if s in tf_map :
		return tf_map[s]
	try :
		return ast.literal_eval(s)
	except :
		return s

class MetaReCsv(MetaGeneric) :
	def __init__(self) :

		# pour l'instant on a rien à stocker...
		self._m = list()

class DataReCsv(DataGeneric) :

	_time = "Timestamp"

	def __init__(self, data_pth:Path, dialect='excel') :
		self.data_pth = data_pth
		self.meta = MetaReCsv()

		self._load(dialect)

	def _load(self, dialect) :
		data_map = collections.defaultdict(list)
		with self.data_pth.open('r', encoding='utf-8') as fid :
			for n, row in enumerate(csv.reader(fid, dialect=dialect)) :
				if n == 0 : # first row
					header = [h.strip() for h in row if h.strip()]
					continue
				for i, (key, val) in enumerate(zip(header, row)) :
					data_map[key].append(round(1000.0 * datetime.datetime.fromisoformat(val).timestamp()) if key == self._time else eval(val))

		self.meta._m = header

		self._d = dict()
		for k in data_map :
			self._d[k] = np.array(data_map[k])
		self._d[self._time] = self._d[self._time] - self._d[self._time][0]

	def __getitem__(self, name) :
		return self._d[name]

	def __len__(self) :
		return len(self._d[self._time])
	
if __name__ == '__main__' :
	import sys
	u = DataReCsv(Path(sys.argv[1]))