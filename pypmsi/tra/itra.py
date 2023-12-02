
import polars as pl
from pypmsi.utils import *
import re
import polars.selectors as cs

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

    if ('{annee}{mois:02}'.format(annee = annee, mois = mois) >= '202303') & (champ == 'mco'):
        df = (
            pl.read_csv(file_in, has_header=False, skip_rows=0, 
                        separator = ";", dtypes= [pl.Utf8 for i in range(7)],
                        new_columns=['cle_rsa', 'norss', 'nas', 
                                     'dtent', 'dtsort', 'ghm1', 'filler'], 
                        n_rows = n_rows)
            .with_columns(pl.all().str.strip_chars())
            .with_columns(cs.starts_with('dt').str.strptime(pl.Date, format="%d%m%Y", strict=False))
            .drop('filler')
        )
        return df

    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
    
    df = parse_pmsi_fwf(df, champ, table, 'xxxx')

    return df

