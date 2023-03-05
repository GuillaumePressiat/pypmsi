
import polars as pl
from pypmsi.utils import *


def irsf(finess, annee : int, mois : int, path : str, ini : bool = True, filepath = "", n_rows = None):

    
    if ini:
        ext = "rsf.ini.txt"
    else:
        ext = "rsf.txt"

    if filepath != "":
        file_in = filepath
    else:
        file_in = (
            path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + ext
        )
    
    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
    
    rsf = dict()

    if ((annee > 2016) & (annee < 2020)):
        typi_r = 2
    else:
        typi_r = 0

    for typer in ['A', 'B', 'C', 'H', 'L', 'M',  'P']:
        df_temp = df.filter(pl.col('l').str.slice(typi_r, 1) == typer)
        rsf[typer] = parse_pmsi_trsf(df_temp,  'rsf', 'rsf', annee, typer)
        
        rsf[typer] = parse_numerics(rsf[typer], '(^mt)|(^tt)|pu|tarif|coeff|taux', 2)
        rsf[typer] = parse_integers_regex(rsf[typer], 'quant|qte')


    return rsf

