#!/usr/bin/env python3

import ast
import collections
import math
import re

from cc_pathlib import Path

from structarray.common import *

from abc import ABC, abstractmethod

class MetaGeneric(ABC) :
	"""
	Classe abstrate pour le gestionnaire de méta données

	Une class meta maintient un dictionnaire (self._m) dont les clés sont:
	  * La clé: le chemin (complet, pas de version compacte ici)
	  * la valeur: un truc (le truc est implémenté dans les sous classes)
	gère la compaction / décompaction des noms
	gère la recherche
	gère name et sizeof, gère le calcul de block_len

	Le reste est délégué
	"""
	block_align = 8
	
	def __init__(self, name, sizeof) :
		self._m = collections.OrderedDict() # chemin complet séparé par des points -> truc
		
		self.name = name
		self.sizeof = sizeof

	def __setitem__(self, key, value) :
		self._m[key] = value

	def __getitem__(self, key) :
		return self._m[key]

	def __len__(self) :
		return len(self._m)

	def __iter__(self) :
		for k, v in self._m.items() :
			yield k, v

	@property
	def block_len(self) :
		if self.sizeof % 8 :
			return (((self.meta.sizeof // 8) + 1) * 8)
		return self.sizeof

	def _proc_name_compact(self) :
		""" iterateur instancié au début et appelé avec .send() pour avoir les valeurs suivantes
		il faut l'appeler dans l'ordre sinon ça n'a aucun sens
		"""
		p_lst = list()
		k = yield None
		while True :
			n_lst = k.split('.')
			q = 0
			for p, n in zip(p_lst, n_lst) :
				if p != n :
					break
				q += 1
			k = yield (f"{q}/" if q else '') + '.'.join(n_lst[q:])
			p_lst = n_lst

	def _proc_name_expand(self) :
		""" iterateur instancié au début et appelé avec .send() pour avoir les valeurs suivantes
		il faut l'appeler dans l'ordre sinon ça n'a aucun sens
		"""
		p_lst = list()
		r = yield None
		while True :
			if '/' in r :
				c, sep, z = r.partition('/')
				n_lst = p_lst[:int(c)] + z.split('.')
				r = yield '.'.join(n_lst)
			else :
				n_lst = r.split('.')
				r = yield r
			p_lst = n_lst


