


"""
proposition d'un objet qui stocke toutes les données de debug_info et qui est capable de répondre à des requuêtes,
faire un arbre de structure, les enumérer, trouver les offset etc.

"""

class DebugInfo() :
	def __init__(self) :
		self.global_map = dict() # name of a global variable -> type of the global variable

		self.alias_map = dict() # name of the alias to a type -> name of the type

		self.basetype_set = dict() # name of basic types -> size in octets, letter 
		
		self.struct_map = dict() # name of the structure -> list of (member name, member type, member offset)
		self.array_map = dict() # name of the array -> (item type, item number)


	

