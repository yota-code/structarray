#!/usr/bin/env python3

import pickle

def pickle_to_str(obj, ** kwarg) :
	return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

def pickle_from_str(txt) :
	return pickle.loads(txt)

