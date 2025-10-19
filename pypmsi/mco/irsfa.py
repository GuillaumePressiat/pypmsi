
import polars as pl
from pypmsi.utils import *


def irsfa(finess, annee : int, mois : int, path : str, filepath = "", n_rows = None):

    
    if filepath != "":
        file_in = filepath
    else:
        file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco.rsface', 'rsfa')
    
    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
    
    rsf = dict()

    if ((annee > 2016) & (annee < 2020)):
        typi_r = 10
    else:
        typi_r = 8

    for typer in ['A', 'B', 'C', 'H', 'L', 'M',  'P']:
        df_temp = df.filter(pl.col('l').str.slice(typi_r, 1) == typer)
        rsf[typer] = parse_pmsi_trsf(df_temp,  'rsf', 'rafael', annee, typer)
        
        rsf[typer] = parse_numerics(rsf[typer], '(^mt)|(^tt)|pu|tarif|coeff|taux', 2)
        rsf[typer] = parse_integers_regex(rsf[typer], 'quant|qte|delai')


    return rsf

