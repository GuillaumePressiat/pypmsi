# pypmsi

Lire les fichiers du PMSI avec python / pola.rs

[![logotest](https://img.shields.io/badge/polars-pypmsi-blue)]()

### temps de lecture de fichiers

#### avec puce silicon

[![](https://img.shields.io/badge/100&nbsp;000&nbsp;rsa-~700ms-firebrick)]()  [![](https://img.shields.io/badge/100&nbsp;000&nbsp;rss-~500ms-firebrick)]()  [![](https://img.shields.io/badge/10&nbsp;000&nbsp;000&nbsp;rsf-~7sec-firebrick)]()

#### avec puce "classique"

[![](https://img.shields.io/badge/100&nbsp;000&nbsp;rsa-~3sec-firebrick)]()  [![](https://img.shields.io/badge/100&nbsp;000&nbsp;rss-~2sec-firebrick)]()


## Installation


```sh
git clone https://github.com/GuillaumePressiat/pypmsi.git
poetry install
```

avec pip

```sh
pip install https://github.com/GuillaumePressiat/pypmsi/releases/latest/download/pypmsi-0.1.7-py3-none-any.whl
```


## Utilisation

```python
import polars
import pypmsi as pm
```

### 3 manières de lire un fichier

##### Spécifier les paramètres dans la fonction

```python
rsa = pm.irsa(290000017, 2021, 5, '~/Documents/data/mco', typi = 4)
rsa
```

##### Définir un noyau de paramètres

```python
p = pm.noyau_pmsi(finess = 290000017, annee = 2021, mois = 5, path = '~/Documents/data/mco')
rsa = p.irsa()
rsa
```

##### indiquer le chemin du fichier et l'année, et le lire

```python
mon_rsa = pm.chemin_pmsi(filepath = '~/Documents/data/mco/290000017.2021.5.rsa', annee = 2021)
rsa = mon_rsa.read_rsa()
rsa
```

(du coup le nom du fichier peut-être formaté différement).


On peut modifier en ligne les paramètres, exemple :

```python
p = pm.noyau_pmsi(finess = 290000017, annee = 2021, mois = 12, path = '~/Documents/data/mco')
# lire les données 2022
rsa = p.irsa(annee = 2022)
rsa
```


### Exemple d'affichage 

```python
rsa
```


```
{'rsa': shape: (57140, 88)
┌───────────┬────────┬────────────┬────────┬─────┬───────┬──────┬─────┬─────────┐
│ nofiness  ┆ novrsa ┆ cle_rsa    ┆ novrss ┆ ... ┆ dr    ┆ ndas ┆ na  ┆ filler6 │
│ ---       ┆ ---    ┆ ---        ┆ ---    ┆     ┆ ---   ┆ ---  ┆ --- ┆ ---     │
│ str       ┆ str    ┆ str        ┆ str    ┆     ┆ str   ┆ i32  ┆ i32 ┆ str     │
╞═══════════╪════════╪════════════╪════════╪═════╪═══════╪══════╪═════╪═════════╡
│ 290000017 ┆ 226    ┆ 00000xxxxx ┆ 120    ┆ ... ┆ R5210 ┆ 0    ┆ 0   ┆         │
│ 290000017 ┆ 226    ┆ 00000xxxxx ┆ 120    ┆ ... ┆ G628  ┆ 0    ┆ 0   ┆         │
│ 290000017 ┆ 226    ┆ 00000xxxxx ┆ 120    ┆ ... ┆ M341  ┆ 0    ┆ 5   ┆         │
│ 290000017 ┆ 226    ┆ 00000xxxxx ┆ 120    ┆ ... ┆       ┆ 16   ┆ 27  ┆         │
│ ...       ┆ ...    ┆ ...        ┆ ...    ┆ ... ┆ ...   ┆ ...  ┆ ... ┆ ...     │
│ 290000017 ┆ 226    ┆ 00000xxxxx ┆ 120    ┆ ... ┆       ┆ 0    ┆ 4   ┆         │
│ 290000017 ┆ 226    ┆ 00000xxxxx ┆ 120    ┆ ... ┆ N185  ┆ 0    ┆ 1   ┆         │
│ 290000017 ┆ 226    ┆ 00000xxxxx ┆ 120    ┆ ... ┆ C504  ┆ 0    ┆ 1   ┆         │
│ 290000017 ┆ 226    ┆ 00000xxxxx ┆ 120    ┆ ... ┆       ┆ 6    ┆ 25  ┆         │
└───────────┴────────┴────────────┴────────┴─────┴───────┴──────┴─────┴─────────┘, 

'actes': shape: (166028, 13)
┌────────────┬───────┬─────────┬────────┬─────┬────────┬────────┬────────┬─────────┐
│ cle_rsa    ┆ delai ┆ cdccam  ┆ descri ┆ ... ┆ assonp ┆ nbexec ┆ indval ┆ nseqrum │
│ ---        ┆ ---   ┆ ---     ┆ ---    ┆     ┆ ---    ┆ ---    ┆ ---    ┆ ---     │
│ str        ┆ i32   ┆ str     ┆ str    ┆     ┆ str    ┆ i32    ┆ str    ┆ str     │
╞════════════╪═══════╪═════════╪════════╪═════╪════════╪════════╪════════╪═════════╡
│ 00000xxxxx ┆ 0     ┆ GLQP002 ┆        ┆ ... ┆ 1      ┆ 1      ┆ 1      ┆ 01      │
│ 00000xxxxx ┆ 0     ┆ PBQM003 ┆        ┆ ... ┆        ┆ 1      ┆ 1      ┆ 01      │
│ 00000xxxxx ┆ 0     ┆ YYYY076 ┆        ┆ ... ┆ 2      ┆ 1      ┆ 1      ┆ 01      │
│ 00000xxxxx ┆ 0     ┆ ZZQX069 ┆        ┆ ... ┆ 4      ┆ 1      ┆ 1      ┆ 01      │
│ ...        ┆ ...   ┆ ...     ┆ ...    ┆ ... ┆ ...    ┆ ...    ┆ ...    ┆ ...     │
│ 00000xxxxx ┆ 4     ┆ DEQP004 ┆        ┆ ... ┆        ┆ 1      ┆ 1      ┆ 02      │
│ 00000xxxxx ┆ 4     ┆ YYYY020 ┆        ┆ ... ┆        ┆ 1      ┆ 1      ┆ 02      │
│ 00000xxxxx ┆ 4     ┆ YYYY020 ┆        ┆ ... ┆        ┆ 1      ┆ 1      ┆ 02      │
│ 00000xxxxx ┆ 4     ┆ YYYY020 ┆        ┆ ... ┆        ┆ 1      ┆ 1      ┆ 02      │
└────────────┴───────┴─────────┴────────┴─────┴────────┴────────┴────────┴─────────┘, 

'diags': shape: (177176, 4)
┌────────────┬─────────┬───────┬──────────┐
│ cle_rsa    ┆ nseqrum ┆ diag  ┆ position │
│ ---        ┆ ---     ┆ ---   ┆ ---      │
│ str        ┆ str     ┆ str   ┆ i32      │
╞════════════╪═════════╪═══════╪══════════╡
│ 00000xxxxx ┆ 01      ┆ Z4180 ┆ 1        │
│ 00000xxxxx ┆ 01      ┆ Z512  ┆ 1        │
│ 00000xxxxx ┆ 01      ┆ Z092  ┆ 1        │
│ 00000xxxxx ┆ 01      ┆ D462  ┆ 1        │
│ ...        ┆ ...     ┆ ...   ┆ ...      │
│ 00000xxxxx ┆ 01      ┆ M0699 ┆ 4        │
│ 00000xxxxx ┆ 01      ┆ C629  ┆ 4        │
│ 00000xxxxx ┆ 01      ┆ N185  ┆ 4        │
│ 00000xxxxx ┆ 01      ┆ C504  ┆ 4        │
└────────────┴─────────┴───────┴──────────┘, 

'rsa_um': shape: (63199, 17)
┌────────────┬─────────┬────────┬───────────┬─────┬─────────┬─────────┬──────────┬─────────┐
│ cle_rsa    ┆ nseqrum ┆ nsequm ┆ nohop1    ┆ ... ┆ nbsupp1 ┆ typaut2 ┆ natsupp2 ┆ nbsupp2 │
│ ---        ┆ ---     ┆ ---    ┆ ---       ┆     ┆ ---     ┆ ---     ┆ ---      ┆ ---     │
│ str        ┆ str     ┆ str    ┆ str       ┆     ┆ i32     ┆ str     ┆ str      ┆ str     │
╞════════════╪═════════╪════════╪═══════════╪═════╪═════════╪═════════╪══════════╪═════════╡
│ 00000xxxxx ┆ 01      ┆ 0028   ┆ 29000xxxx ┆ ... ┆ 0       ┆         ┆          ┆         │
│ 00000xxxxx ┆ 01      ┆ 0021   ┆ 29000xxxx ┆ ... ┆ 0       ┆         ┆          ┆         │
│ 00000xxxxx ┆ 01      ┆ 0022   ┆ 29000xxxx ┆ ... ┆ 0       ┆         ┆          ┆         │
│ 00000xxxxx ┆ 01      ┆ 0076   ┆ 29000xxxx ┆ ... ┆ 109     ┆         ┆          ┆         │
│ ...        ┆ ...     ┆ ...    ┆ ...       ┆ ... ┆ ...     ┆ ...     ┆ ...      ┆ ...     │
│ 00000xxxxx ┆ 02      ┆ 0039   ┆ 29000xxxx ┆ ... ┆ 0       ┆         ┆          ┆         │
│ 00000xxxxx ┆ 03      ┆ 0039   ┆ 29000xxxx ┆ ... ┆ 0       ┆         ┆          ┆         │
│ 00000xxxxx ┆ 04      ┆ 0085   ┆ 29000xxxx ┆ ... ┆ 0       ┆         ┆          ┆         │
│ 00000xxxxx ┆ 05      ┆ 0085   ┆ 29000xxxx ┆ ... ┆ 0       ┆         ┆          ┆         │
└────────────┴─────────┴────────┴───────────┴─────┴─────────┴─────────┴──────────┴─────────┘}
```


### Quelques statistiques avec polars


```python
(rsa['actes']
	.filter(pl.col('cdccam').str.contains('EBLA'))
	.group_by(['cdccam', 'nbexec'])
	.count()
)
```

```
shape: (2, 3)
┌─────────┬────────┬───────┐
│ cdccam  ┆ nbexec ┆ count │
│ ---     ┆ ---    ┆ ---   │
│ str     ┆ i64    ┆ u32   │
╞═════════╪════════╪═══════╡
│ EBLA002 ┆ 1      ┆ 2     │
│ EBLA003 ┆ 1      ┆ 185   │
└─────────┴────────┴───────┘
```

```python
(rsa['actes']
	.filter(pl.col('cdccam').str.contains('EBLA'))
	.join(rsa['rsa'], on = 'cle_rsa', how = 'inner')
	.pivot('nbexec', 'cdccam', 'rsatype', 'count')
	.fill_null(0)
)
```

```
shape: (2, 5)
┌─────────┬─────┬─────┬─────┬─────┐
│ cdccam  ┆ C   ┆ M   ┆ Z   ┆ K   │
│ ---     ┆ --- ┆ --- ┆ --- ┆ --- │
│ str     ┆ u32 ┆ u32 ┆ u32 ┆ u32 │
╞═════════╪═════╪═════╪═════╪═════╡
│ EBLA003 ┆ 24  ┆ 56  ┆ 9   ┆ 96  │
│ EBLA002 ┆ 0   ┆ 1   ┆ 1   ┆ 0   │
└─────────┴─────┴─────┴─────┴─────┘
```

```python
(rsa['actes']
	.filter(pl.col('cdccam').str.contains('EBLA'))
	.join(rsa['rsa'], on = 'cle_rsa', how = 'inner')
	.pivot('nbexec', 'cdccam', ['rsacmd', 'rsatype'], 'sum')
	.fill_null(0)
)
```

```
shape: (2, 19)
┌─────────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐
│ cdccam  ┆ 17  ┆ 11  ┆ 04  ┆ ... ┆ C   ┆ M   ┆ Z   ┆ K   │
│ ---     ┆ --- ┆ --- ┆ --- ┆     ┆ --- ┆ --- ┆ --- ┆ --- │
│ str     ┆ i64 ┆ i64 ┆ i64 ┆     ┆ i64 ┆ i64 ┆ i64 ┆ i64 │
╞═════════╪═════╪═════╪═════╪═════╪═════╪═════╪═════╪═════╡
│ EBLA003 ┆ 29  ┆ 1   ┆ 13  ┆ ... ┆ 24  ┆ 56  ┆ 9   ┆ 96  │
│ EBLA002 ┆ 0   ┆ 0   ┆ 0   ┆ ... ┆ 0   ┆ 1   ┆ 1   ┆ 0   │
└─────────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘
```

