# RSF

Lecture des fichiers RSF, résumés standardisés de facturation.

## Syntaxe

```python
irsf(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	ini : bool,
	filepath = "", 
	n_rows = None)
```


## Paramètres

- finess : le finess juridique du fichier rsf
- annee : l'année de la période PMSI
- mois  : le mois de la période PMSI
- path : le repertoire d'accès au fichier
- ini : si vrai, on lit le fichier en entrée de preface sans réindexation des numéros séquentiels de factures
- filepath : chemin du fichier complémentaire si import d'un fichier direct sans les autres paramètres
- n_rows : nombre de lignes si import limité




