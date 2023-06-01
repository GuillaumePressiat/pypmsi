# Noyau de paramètres


### 3 manières de lire un fichier

##### Spécifier les paramètres dans la fonction

```python
rsa = pm.irsa(
    290000017, 
    2021,
    5, 
    '~/Documents/data/mco', 
    typi = 4)
rsa
```

##### Définir un noyau de paramètres

```python
p = pm.noyau_pmsi(
    finess = 290000017, 
    annee = 2021, 
    mois = 5, 
    path = '~/Documents/data/mco')

rsa = p.irsa()
rsa
```

##### indiquer le chemin du fichier et l'année, et le lire

```python
mon_rsa = pm.chemin_pmsi(
    filepath = '~/Documents/data/mco/290000017.2021.5.rsa', 
    annee = 2021)

rsa = mon_rsa.read_rsa()
rsa
```

(du coup le nom du fichier peut-être formaté différement).


On peut modifier en ligne les paramètres, exemple :

```python
p = pm.noyau_pmsi(
    finess = 290000017, 
    annee = 2021, 
    mois = 12, 
    path = '~/Documents/data/mco')

# lire les données 2022
rsa = p.irsa(annee = 2022)
rsa
```

