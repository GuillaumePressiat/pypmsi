
import polars as pl
from pypmsi.utils import *

def prepare_rsa_pour_requetes(rsa):
    
    rsa['rsa'] = rsa['rsa'].with_columns(
        pl.when(pl.col("agean") == None)
        .then(pl.col('agejr') / 365.25)
        .otherwise(pl.col("agean")).alias("agean")
        )
    return rsa


def requete_rsa(rsa, elements):
    query_liste = dict()
    
    if 'ghm' in elements:
        query_liste['ghm'] = pl.col('ghm').str.contains('|'.join(elements['ghm']))
    if 'ghm_exclus' in elements:
        query_liste['ghm_exclus'] = ~ pl.col('ghm').str.contains('|'.join(elements['ghm_exclus']))
    if 'diags_exclus' in elements:
        query_liste['diags_exclus'] = ~ pl.col('diags').str.contains('|'.join(elements['diags_exclus']))
    if 'agemin' in elements:
        query_liste['agemin'] = (pl.col('agean') >= elements['agemin'])
    if 'agemax' in elements:
        query_liste['agemax'] = (pl.col('agean') <= elements['agemax'])
    if 'agejrmin' in elements:
        query_liste['agejrmin'] = (pl.col('agejr') >= elements['agejrmin'])
    if 'agejrmax' in elements:
        query_liste['agejrmax'] = (pl.col('agejr') <= elements['agejrmax'])
    if 'dureemin' in elements:
        query_liste['dureemin'] = (pl.col('duree') >= elements['dureemin'])
    if 'dureemax' in elements:
        query_liste['dureemax'] = (pl.col('duree') <= elements['dureemax'])
    if 'poidsmin' in elements:
        query_liste['poidsmin'] = (pl.col('poids') >= elements['poidsmin'])
    if 'poidsmax' in elements:
        query_liste['poidsmax'] = (pl.col('poids') <= elements['poidsmax'])
    
    if len(query_liste) == 0:
        query_liste = {'fill_le_boolean' : True}
    
    temp = rsa['rsa'].lazy()
    for filtre in query_liste.values():
        temp = temp.filter(filtre)

    if 'positions_diags' in elements:
        if elements['positions_diags'][0] == 'toutes':
            diag_pos = {'fill_le_boolean' : True}
        elif elements['positions_diags'][0] == 'dp':
            diag_pos = (pl.col('position') == 1)
        else:
            diag_pos = (pl.col('position').is_in(elements(['positions_diags'])))

    if 'diags' in elements:
        if len(elements['diags']) == 1:
            pattern_diags = elements['diags'][0]
        else:
            pattern_diags = '|'.join(elements['diags'])
        d = (
            rsa['diags'].lazy()
            .filter(pl.col('diag').str.contains(pattern_diags))
            .filter(diag_pos)
        ).unique('cle_rsa').select('cle_rsa')
        temp = temp.join(d, on = 'cle_rsa', how = 'inner')    

    
    if 'actes' in elements:
        if len(elements['actes']) == 1:
            pattern_actes = elements['actes'][0]
        else:
            pattern_actes = '|'.join(elements['actes'])
        
        a = rsa['actes'].lazy().filter(pl.col('cdccam').str.contains(pattern_actes)).unique('cle_rsa').select('cle_rsa')
        temp = temp.join(a, on = 'cle_rsa', how = 'inner')

    
    return temp.collect()


