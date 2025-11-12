# structarray

We propose here a format to dump and decode any C structure, even if it is made itself of other structures or arrays (to be implemented)

The idea is:

* to record the structure (this was intended to be the scade context structure) each cycle, in a file, as this.
* to map the structure (each variable is associated to an address and a type)
* to decode the recorded file in order to convert it eventually in .tsv for analysis

## Meta

Le fichier descriptif est toujours un fichier `.tsv` mais il possède plusieurs variantes dans son format. Le nom des variables peut être présenté sous la forme complète ou compacte et les adresses des variables sous la forme absolue ou relative, chacunes ayant leur avantages.

La première ligne du fichier contient: 

* le nom de la structure (pour référence, ce champ est quasi libre et sans réelle portée)
* la taille de la structure (la vraie taille, celle que retournerai un sizeof() en C)

La taille du bloc en revanche peut-être légèrement supérieure. Si celle-ci n'est pas un multiple de 8 le bloc devra être paddé.


## futurologie

* pouvoir gérer plusieurs fichiers de donnée en même temps :
	- des fichiers d'agents différents
	- ou plusieurs fichiers d'un même agent (même contexte)

ok, mais comment on gère la recherche multi agent

* par défaut les agents sont synchronisés sur leurs horloges respectives.
	- sinon, on est capable de faire une synchro par inter correlation
* possibilité de recentrer tous les plots sur un trigger ou une plage de temps
* possibilité de définir des trigger (aussi dans le bookmark)
* comment je transforme les données


- rafraichir le contenu des fenêtre si on change le trigger ou les données
- trigger !
- synchro inter modèle