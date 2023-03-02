
import polars as pl
from pypmsi.utils import *


def irsf(finess, annee : int, mois : int, path : str, ini : bool = True):

    
    if ini:
        ext = "rsf.ini.txt"
    else:
        ext = "rsf.txt"

    file_in = (
        path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + ext
    )
    
    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"])
    
    rsf = dict()

    if ((annee > 2016) & (annee < 2020)):
        typi_r = 2
    else:
        typi_r = 0

    for typer in ['A', 'B', 'C', 'H', 'L', 'M',  'P']:
        df_temp = df.filter(pl.col('l').str.slice(typi_r, 1) == typer)
        rsf[typer] = parse_pmsi_trsf(df_temp,  'rsf', 'rsf', annee, typer)
        
        rsf[typer] = parse_numerics(rsf[typer], '(^mt)|(^tt)|pu|tarif|coeff|taux', 2)
        rsf[typer] = parse_numerics(rsf[typer], 'quant|qte', 0)


    return rsf

