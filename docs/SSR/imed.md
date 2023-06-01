# MED

Lecture des fichiers MED, DMI, médicaments dans le champ SSR

## Syntaxe

```python
imed_ssr(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	typmed : str, 
	filepath = "", 
	n_rows = None)
```

## Types d'imports 

Le paramètre `typmed` permet de choisir le type d'import entre les fichiers du in ou du out. 


## Paramètres

- finess : le finess juridique des fichiers complémentaires
- annee : l'année de la période PMSI
- mois  : le mois de la période PMSI
- path : le repertoire d'accès au fichier
- typmed/typdmi : le type de fichier (in / out)
- filepath : chemin du fichier complémentaire si import d'un fichier direct sans les autres paramètres
- n_rows : nombre de lignes si import limité




