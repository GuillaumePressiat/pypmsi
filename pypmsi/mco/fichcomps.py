import polars as pl
from pypmsi.utils import *


def imed_mco(finess, annee : int, mois : int, path : str, typmed : str = "in", filepath = "", n_rows = None) -> pl.DataFrame:
    """Découper le fichier med du in ou du out
    
    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typmed (str, optional): Description
    
    Returns:
        pl.DataFrame: Dataframe contenant les médicaments T2A, ATU / APAC / thrombo du in ou du out
    """
    if typmed == "in":
        if filepath != "":
            file_in = filepath
        else:
            file_in = (
                path
                + "/"
                + str(finess)
                + "."
                + str(annee)
                + "."
                + str(mois)
                + "."
                + "med.txt"
            )
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rum_med", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = (
                path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "med*"
            )
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rsa_med", annee)

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


def idmi_mco(finess, annee : int, mois : int, path : str, typdmi : str = "in", filepath = "", n_rows = None) -> pl.DataFrame:
    """Découper le fichier dmi du in ou du out
    
    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typdmi (str, optional): Description
    
    Returns:
        pl.DataFrame: Dataframe contenant les DMI du in ou du out
    """
    if typdmi == "in":
        if filepath != "":
            file_in = filepath
        else:
            file_in = (
                path
                + "/"
                + str(finess)
                + "."
                + str(annee)
                + "."
                + str(mois)
                + "."
                + "dmi.txt"
            )
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rum_dmi", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = (
                path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "dmip"
            )
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rsa_dmi", annee)

    df = (
        df.lazy()
        .with_columns(
            [
                (pl.col("prix").cast(pl.Int64, strict=False) / 1000).alias("prix"),
                pl.col("nbpose").cast(pl.Int64).alias("nbpose"),
            ]
        )
        .collect()
    )

    return df
