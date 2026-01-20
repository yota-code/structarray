#!/usr/bin/env python3

"""
On peut ouvrir jusqu'à 10 fichiers de données, ils auront forcément les couleurs suivantes par défaut :
	tab:blue, tab:orange, tab:green, tab:red, tab:purple, tab:brown, tab:pink, tab:gray, tab:olive, tab:cyan

"""

class BackPlot() :
	def __init__(self, layout, * reb_lst) :

		self.layout = layout
		self.reb_lst = reb_lst

