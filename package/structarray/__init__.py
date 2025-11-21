#!/usr/bin/env python3

from cc_pathlib import Path

import structarray.rebin
import structarray.rezip

def open(data:Path, meta:Path=None) :
	if data.suffix == '.reb' :
		return structarray.rebin.DataRebin(data, meta)
	elif data.suffix == '.rez' :
		return structarray.rebin.DataRezip(data)
	