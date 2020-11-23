#!/usr/bin/env python3

def tsv_from_str(txt) :
	return [[
		cell.replace('\\t', '\t').replace('\\n', '\n').replace('\\\\', '\\').strip()
		for cell in row.split('\t')
	] for row in txt.split('\n') if (row.strip() and row[0] != "#") ]

def tsv_to_str(obj, comment=None) :
	"""
		obj must be a list of list of python objects having a decent str() form
	"""
	comment = '' if comment is None else ('# ' + comment.replace('\n', ' ') + '\n')
	return comment + '\n'.join( '\t'.join(
		('' if cell is None else str(cell)).strip().replace('\\', '\\\\').replace('\n', '\\n').replace('\t', '\\t')
		for cell in row
	) for row in obj )

