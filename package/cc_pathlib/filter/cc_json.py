#!/usr/bin/en python3

import json
import datetime

class JSONCustomEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, set) :
			return sorted(obj)
		elif isinstance(obj, datetime.datetime) :
			return obj.isoformat()
		else:
			return json.JSONEncoder.default(self, obj)

class _JSON_config_CONTEXT() :
	def __init__(self, pth) :
		self.pth = pth

	def __enter__(self) :
		if not self.pth.is_file() :
			self.obj = dict()
		else :
			self.obj = self.pth.load()
		return self.obj

	def __exit__(self, exc_type, exc_value, traceback) :
		self.pth.save(self.obj, filter_opt={"verbose":True})

def json_from_str(txt) :
	return json.loads(txt)

def json_to_str(obj, verbose=False) :
	p = {
		'ensure_ascii' : False,
		'sort_keys' : True,
		'cls' : JSONCustomEncoder
	}
	if verbose :
		p['indent'] = '\t'
	else :
		p['separators'] = (',', ':')
	return json.dumps(obj, ** p)
