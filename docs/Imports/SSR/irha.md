# RHA

Lecture des fichiers RHA depuis 2011, résumés hebdomadaires anonymisés.

## Syntaxe

```python
irha(
	finess, 
	annee : int, 
	mois : int, 
	path : str, 
	typi : int = 4, 
	tdiag : bool = True, 
	filepath = "", 
	n_rows = None)
```

## Types d'imports 

Le paramètre `typi` permet de choisir le type d'import entre : 

4 types d'imports (typi)

- 1 : partie fixe uniquement
- 2 : partie fixe + zones streams actes, das
- **3 : partie fixe + parties variables**
- 4 : partie fixe + parties variables + zones streams actes, das

**3** est l'import par défaut.

## Paramètres

- finess : le finess juridique du fichier rha
- annee : l'année de la période PMSI
- mois  : le mois de la période PMSI
- path : le repertoire d'accès au fichier
- typi : le type d'import
- tdiag : les diagnostics doivent-ils être "rangés dans une table unique"
- filepath : chemin du fichier rsa si import d'un fichier direct sans les autres paramètres
- n_rows : nombre de lignes si import limité




