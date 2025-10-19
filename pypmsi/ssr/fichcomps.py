import polars as pl
from pypmsi.utils import *


def imed_ssr(finess, annee : int, mois : int, path : str, typmed : str = "in", filepath = "", n_rows = None) -> pl.DataFrame:
    """Découper le fichier med du in ou du out
    
    Args:
        finess (str): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typmed (str, optional): Description
    
    Returns:
        pl.DataFrame: Dataframe contenant les médicaments
    """
    if typmed == "in":
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'ssr', 'med.txt')
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "ssr", "rhs_med", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'ssr', 'med*')
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "ssr", "rha_med", annee)

    df = (
        df.lazy()
        .with_columns(
            [
                (pl.col("prix").cast(pl.Int64, strict=False) / 1000).alias("prix"),
                (pl.col("nbadm").cast(pl.Int64, strict=False) / 1000).alias("nbadm"),
            ]
        )
        .collect()
    )

    return df

