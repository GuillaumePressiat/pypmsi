# Exemple de statistiques



## Exemple sur les RSA

```python
import polars as pl
import pypmsi as pm

p = pm.noyau_pmsi(finess = '290000017', 
                  annee = 2022, 
                  mois = 12,
                  path = '/Users/guillaumepressiat/Documents/data/mco')
rsa = p.irsa(p)
# rsa
```

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
    .pivot(index = 'cdccam', values = 'nbexec', on = 'rsatype', aggregate_function = 'sum')
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
    .pivot(index = ['rsacmd', 'rsatype'], values = 'nbexec', on = 'cdccam', 
           separator = '-', aggregate_function = 'sum')
    .fill_null(0)
)
```

```
shape: (39, 4)
┌────────┬─────────┬─────────┬─────────┐
│ rsacmd ┆ rsatype ┆ EBLA003 ┆ EBLA002 │
│ ---    ┆ ---     ┆ ---     ┆ ---     │
│ str    ┆ str     ┆ i32     ┆ i32     │
╞════════╪═════════╪═════════╪═════════╡
│ 15     ┆ C       ┆ 1       ┆ 0       │
│ 01     ┆ M       ┆ 6       ┆ 0       │
│ 17     ┆ M       ┆ 39      ┆ 0       │
│ 16     ┆ M       ┆ 5       ┆ 0       │
│ 17     ┆ K       ┆ 1       ┆ 0       │
│ …      ┆ …       ┆ …       ┆ …       │
│ 18     ┆ M       ┆ 1       ┆ 0       │
│ 19     ┆ M       ┆ 1       ┆ 0       │
│ 21     ┆ C       ┆ 1       ┆ 0       │
│ 11     ┆ C       ┆ 1       ┆ 1       │
│ 21     ┆ M       ┆ 1       ┆ 0       │
└────────┴─────────┴─────────┴─────────┘
```

