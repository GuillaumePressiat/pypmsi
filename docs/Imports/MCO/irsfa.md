# RSFA

Lecture des fichiers RSFA, résumés standardisés de facturation anonymisés contenant les RAFAEL, résumés anonymisés de facturation des actes et consultations externes liés (RSF-ACE).

## Syntaxe

```python
irsfa(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	filepath = "", 
	n_rows = None)
```


## Paramètres

- finess : le finess juridique du fichier RSFA (rafael)
- annee : l'année de la période PMSI
- mois  : le mois de la période PMSI
- path : le repertoire d'accès au fichier
- filepath : chemin du fichier complémentaire si import d'un fichier direct sans les autres paramètres
- n_rows : nombre de lignes si import limité




