# TRA

Lecture des fichiers tra, lien entre le out et le in

## Syntaxe

```python
itra(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	champ : str, 
	filepath = "", 
	n_rows = None)
```

## Types d'imports 

Le paramètre `champ` permet de choisir le type de fichier tra entre les différents champs PMSI parmi :

- mco
- ssr
- had
- psy_rpsa
- psy_r3a 


## Paramètres

- finess : le finess juridique du fichier ano/anohosp
- annee : l'année de la période PMSI
- mois  : le mois de la période PMSI
- path : le repertoire d'accès au fichier
- champ : le champ PMSI (deux spécifiques pour la psy : psy_rpsa, psy_r3a, had, mco, ssr, had)
- filepath : chemin du fichier complémentaire si import d'un fichier direct sans les autres paramètres
- n_rows : nombre de lignes si import limité




