
import polars as pl
from pypmsi.utils import *


def irsfa(finess, annee : int, mois : int, path : str):
    """Découper les RSFA, rafael
    
    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
    
    Returns:
        TYPE: Dictionnaire contenant les différents RSFA A, B, C, H, L, M, P
    """
    file_in = (
        path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "rsfa"
    )
    
    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"])
    
    rsf = dict()

    if ((annee > 2016) & (annee < 2020)):
        typi_r = 10
    else:
        typi_r = 8

    for typer in ['A', 'B', 'C', 'H', 'L', 'M',  'P']:
        df_temp = df.filter(pl.col('l').str.slice(typi_r, 1) == typer)
        rsf[typer] = parse_pmsi_trsf(df_temp,  'rsf', 'rafael', annee, typer)

    return rsf

