# MED / DMI

Lecture des fichiers MED, DMI, médicaments et dispositifs médicaux implantables

## Syntaxe

```python
imed_mco(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	typmed : str, 
	filepath = "", 
	n_rows = None)
```

```python
idmi_mco(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	typdmi : str, 
	filepath = "", 
	n_rows = None)
```

## Types d'imports 

Le paramètre `typ(med|dmi)` permet de choisir le type d'import entre les fichiers du in ou du out. 


## Paramètres

- finess : le finess juridique des fichiers complémentaires
- annee : l'année de la période PMSI
- mois  : le mois de la période PMSI
- path : le repertoire d'accès au fichier
- typmed/typdmi : le type de fichier (in / out)
- filepath : chemin du fichier complémentaire si import d'un fichier direct sans les autres paramètres
- n_rows : nombre de lignes si import limité




