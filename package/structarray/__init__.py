#!/usr/bin/env python3

from cc_pathlib import Path

def open(data:Path, meta:Path=None) :
	match data.suffix :
		case '.reb' :
			import structarray.rebin
			return structarray.rebin.DataRebin(data, meta)
		case '.rez' :
			import structarray.rezip
			return structarray.rezip.DataRezip(data)
		case '.csv' :
			import structarray.recsv
			return structarray.recsv.DataReCsv(data)
		case _ :
			raise NotImplementedError(f"Unknown file type: {data.suffix}")
	