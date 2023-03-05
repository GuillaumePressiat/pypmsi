import polars as pl
from pypmsi.utils import *

def iano_mco(finess, annee : int, mois : int, path : str, typano :str ="in", filepath = "") -> pl.DataFrame:
    """Découper le ano in ou out du mco
    
    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typano (str, optional): Description
    
    Returns:
        pl.DataFrame: Dataframe in ou out contenant le fichier ano du mco
    """
    
    if typano == "in":
        if filepath != "":
            file_in = filepath
        else:
            file_in = (
                path + "/"
                + str(finess)
                + "."
                + str(annee)
                + "."
                + str(mois)
                + "."
                + "ano.txt"
            )
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"])
        df = parse_pmsi_fwf(df, "mco", "rum_ano", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = (
                path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "ano"
            )
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"])
        df = parse_pmsi_fwf(df, "mco", "rsa_ano", annee)

    return df
