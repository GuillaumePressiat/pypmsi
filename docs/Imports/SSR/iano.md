# ANO-HOSP

Lecture des fichiers ano et anohosp, contenant les informations de facturations anonymisées.

## Syntaxe

```python
iano_ssr(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	typano : str, 
	filepath = "", 
	n_rows = None)
```

## Types d'imports 

Le paramètre `typano` permet de choisir le type d'import entre les fichiers du in ou du out. 


## Paramètres

- finess : le finess juridique du fichier ano/anohosp
- annee : l'année de la période PMSI
- mois  : le mois de la période PMSI
- path : le repertoire d'accès au fichier
- typano : le type de fichier (in / out)
- filepath : chemin du fichier complémentaire si import d'un fichier direct sans les autres paramètres
- n_rows : nombre de lignes si import limité




