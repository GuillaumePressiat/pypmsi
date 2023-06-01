
import polars as pl
from pypmsi.utils import *

def irpss(finess, annee : int, mois : int, path : str, typi : int = 1, tdiag : bool = True, filepath = "", n_rows = None) -> dict:
    """Découpage des RPSS ; 201x à 202x
    
    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typi (int, optional): Description
        tdiag (bool, optional): Description
    
    Returns:
        dict: Dictionnaire avec des dataframe
    """

    # 4 types d'imports (typi)
    # # 1          : partie fixe uniquement
    # # 2          : partie fixe + zones streams actes, das
    # # 3 (défaut) : partie fixe + partie variable
    # # 4          : partie fixe + partie variable + zones streams actes, das

    # Transposition des diagnostics (tdiag)
    # # True : renvoie une seule table avec les diags par position, 1 dp, 2 dr, 3 das, 4 dad
    # # False : renvoie deux tables séparés avec les das et les dad
    
    if filepath != "":
        file_in = filepath
    else:
        file_in = (
            path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "rpss.txt"
        )

    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)

    df = parse_pmsi_fwf(df, "had", "rpss", annee)

    return df



