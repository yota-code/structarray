#!/usr/bin/env python3

"""
On peut ouvrir jusqu'à 10 fichiers de données, ils auront forcément les couleurs suivantes par défaut :
	tab:blue, tab:orange, tab:green, tab:red, tab:purple, tab:brown, tab:pink, tab:gray, tab:olive, tab:cyan

"""

class FrontPlot() :
	""" cette classe gère l'interaction utilisateur via un prompt"""

	n_lst = '0123456789abcdefijklmnpqrstuvxyz'

	proc_map ={
		'!' : "command",
		'@' : "bookmark",
		'&' : "textual",
		'#' : "graphical",
	}

	def __init__(self, meta_pth, * data_lst) :
		self.meta = MetaReb().load(meta_pth)
		self.data_lst = data_lst

		self.fig_map = dict() # chaque figure correspond à 1 BackPlot

	def _proc__error__(self, s) :
		print("\x1b[31Invalid Request:\x1b[0m", s)

	def _proc_textual(self, s) :
		pass

	def proc__search__(self, s) :
		v_lst = self.meta.search(s)
		for i, v in enumerate(v_lst[:32]) :
			print(f'\x1b[{33 if i % 2 == 2 else 93}m{self.n_lst[i]}. {v}\x1b[0m')
		
	def _proc_magic(self, s) :
		if s == "q" :
			print("Bye bye!")
			sys.exit(0)
		if s == "r" :
			# TODO : implémenter le rechargement des données (si les fichiers ont changé par exemple 
			pass

	def run(self) :
		while True :
			ans = input("\x1b[35m>>>\x1b[0m ").strip()
			if not ans :
				continue
			try :
				if k in self.proc_map :
					getattr(self, f"_proc_{k}")(ans[1:].strip())
				else :
					self.proc__search__(ans)
			except :
				pass


c_lst = [0, 96, 34, 96]
for i in range(15) :
	print(f"\x1b[{c_lst[i % len(c_lst)]}m{i:2d}. blablablabla blabla bla\x1b[0m")