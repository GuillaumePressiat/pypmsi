# Valoriser les RSA comme e-PMSI



```python
import pypmsi as pm
import polars as pl
import refpymsi as rp
```

### Paramètres et lecture des données

```python
p = pm.noyau_pmsi(finess = 290000017, 
              annee  = 2022,
              mois   = 12,
              path   = '/Users/guillaumepressiat/Documents/data/mco')

rsa = p.irsa(typi = 4)

vrsa = pm.vvs_rsa(rsa)
vano = pm.vvs_ano_mco(p.iano_mco())
```


### Tarifs ghs et suppléments

On utilise le package [refpymsi](https://github.com/GuillaumePressiat/refpymsi).

```python
tarifs = rp.get_table('tarifs_mco_ghs')
supplements = rp.get_table('tarifs_mco_supplements')
```

### Calculs de valorisations

```python
python_valo_sej = pm.vvs_mco(pm.vvs_ghs_supp(vrsa,
                                      tarifs, 
                                      supplements, 
                                      bee = False, 
                                      diap = p.idiap_mco(), 
                                      pie = p.ipie_mco(), 
                                      porg = p.ipo()),
                      pm.vvs_mco_sv(vrsa, 
                                    vano, 
                                    porg = p.ipo())
          )

```


### Tableau SV - Séjours valorisés e-PMSI

```python
pm.vvs_mco_epmsi_sv(python_valo_sej)
```

### Tableau RAV - Récapitulatif Activité valorisation e-PMSI

```python
pm.vvs_mco_epmsi_rav(python_valo_sej)
```

