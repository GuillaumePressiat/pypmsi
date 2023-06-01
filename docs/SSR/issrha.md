# SSRHA

Lecture des fichiers SSRHA depuis 2011, "suites semestrielles" de RHA.

## Syntaxe

```python
issrha(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	filepath = "", 
	n_rows = None)
```

## Paramètres

- finess : le finess juridique du fichier ssrha
- annee : l'année de la période PMSI
- mois  : le mois de la période PMSI
- path : le repertoire d'accès au fichier
- filepath : chemin du fichier rsa si import d'un fichier direct sans les autres paramètres
- n_rows : nombre de lignes si import limité

