
import polars as pl
from pypmsi.utils import *
import re

def itra(finess, annee : int, mois : int, path : str, champ : str = "mco", filepath = "", n_rows = None):

    
    if champ in ["mco","had", "psy_rpsa"]:
        ext = 'tra.txt'

    if champ == "ssr" :
        ext = 'tra'

    if champ == "psy_r3a" :
        ext = 'tra.raa.txt'
    
    if re.search('psy', champ):
        table = champ
        champ = "psy"
    else:
        table = "tra"

    
    
    if filepath != "":
        file_in = filepath
    else:
        file_in = (
            path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + ext
        )
    
    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
    
    df = parse_pmsi_fwf(df, champ, table, 'xxxx')

    return df

