
import polars as pl
from pypmsi.utils import *


def irsf(finess, annee : int, mois : int, path : str, ini : bool = True):
    """Découper le fichier RSF
    
    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        ini (bool, optional): Lire le fichier rsf.ini.txt (True) ou le fichier rsf.txt
    
    Returns:
        TYPE: Dictionnaire contenant les différents RSF A, B, C, H, L, M, P
    """
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

    return rsf

