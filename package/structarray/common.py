#!/usr/bin/enb python3

sizeof_map = { # size of types
	'N1' : 1,
	'N2' : 2,
	'N4' : 4,
	'N8' : 8,
	'Z1' : 1,
	'Z2' : 2,
	'Z4' : 4,
	'Z8' : 8,
	'R4' : 4,
	'R8' : 8,
	'P4' : 4,
	'P8' : 8,
}

ntype_map = { # types numpy
	'Z1' : "int8",
	'Z2' : "int16",
	'Z4' : "int32",
	'Z8' : "int64",
	'N1' : "uint8",
	'N2' : "uint16",
	'N4' : "uint32",
	'N8' : "uint64",
	'R4' : "float32",
	'R8' : "float64",
}

stype_map = { # types of struct
	'Z1' : "b",
	'Z2' : "h",
	'Z4' : "i",
	'Z8' : "q",
	'N1' : "B",
	'N2' : "H",
	'N4' : "I",
	'N8' : "Q",
	'R4' : "f",
	'R8' : "d",
}


def globex_to_regex(s) :
	s = s.replace('\\.', '\0')
	s = s.replace('.', '\\.')
	s = s.replace('*', '.*')
	s = s.replace('\0', '.')
	return s
