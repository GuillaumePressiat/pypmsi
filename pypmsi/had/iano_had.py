import polars as pl
from pypmsi.utils import *

def iano_had(finess, annee : int, mois : int, path : str, typano :str ="in", filepath = "", n_rows = None) -> pl.DataFrame:
    """Découper le ano in ou out de l'had
    
    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typano (str, optional): Description
    
    Returns:
        pl.DataFrame: Dataframe in ou out contenant le fichier ano de l'had
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
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "had", "rpss_ano", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = (
                path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "ano"
            )
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "had", "rapss_ano", annee)

    return df
