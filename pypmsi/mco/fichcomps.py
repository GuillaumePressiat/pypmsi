import polars as pl
from pypmsi.utils import *


def imed_mco(finess, annee : int, mois : int, path : str, typmed : str = "in", filepath = "", n_rows = None) -> pl.DataFrame:
    """Découper le fichier med du in ou du out
    
    Args:
        finess (str): Description
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
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', 'med.txt')
        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rum_med", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', 'med*')
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
        finess (str): Description
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
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', "dmi.txt")

        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rum_dmi", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', "dmip")

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


def idiap_mco(finess, annee : int, mois : int, path : str, typdiap : str = "out", filepath = "", n_rows = None) -> pl.DataFrame:
    """Découper le fichier diap du in ou du out
    
    Args:
        finess (str): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typdiap (str, optional): Description
    
    Returns:
        pl.DataFrame: Dataframe contenant les dialyses péritéonéales (fichcomp) du in ou du out
    """
    if typdiap == "in":
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', "diap.txt")

        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "ffc_in", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', "diap")

        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rsa_diap", annee)


    return df


def ipie_mco(finess, annee : int, mois : int, path : str, typpie : str = "out", filepath = "", n_rows = None) -> pl.DataFrame:
    """Découper le fichier PIE du out
    
    Args:
        finess (str): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typpie (str, optional): Description
    
    Returns:
        pl.DataFrame: Dataframe contenant les PIE (fichcomp) du du out
    """
    if typpie == "in":
        return None
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', "pie")

        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rsa_pie", annee).with_columns(pl.col('nbsuppie').cast(pl.Int32))


    return df

def ipo(finess, annee : int, mois : int, path : str, typpo : str = "out", filepath = "", n_rows = None) -> pl.DataFrame:
    """Découper le fichier PORG du out
    
    Args:
        finess (str): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typpo (str, optional): Description
    
    Returns:
        pl.DataFrame: Dataframe contenant les PORG (fichcomp) du du out
    """
    if typpo == "in":
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', "porg.txt")

        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "ffc_in", annee)
    else:
        if filepath != "":
            file_in = filepath
        else:
            file_in = path + '/' + pmsi_format_fullname(finess, annee, mois, 'mco', "porg")

        df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)
        df = parse_pmsi_fwf(df, "mco", "rsa_po", annee)


    return df
