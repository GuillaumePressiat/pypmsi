
import polars as pl
from pypmsi.utils import *

def issrha(finess, annee : int, mois : int, path : str, tdiag : bool = False, filepath = "", n_rows = None) -> dict:
    """Découpage des SSRHA ; 2011 à 2023
    
    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        tdiag (bool, optional): Description
    
    Returns:
        dict: Dictionnaire avec des dataframe
    """
    
    if filepath != "":
        file_in = filepath
    else:
        file_in = (
            path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "sha"
        )

    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)

    df = parse_pmsi_fwf(df, "ssr", "ssrha", annee)

    # résultat
    
    ssrha = df.drop('zgp')
    
    if str(annee) < '2018':
        ssrha = {"ssrha": ssrha}
        return ssrha 

    patterns_gme = get_patterns(str(annee), "ssrha_gme")
    zgme = patterns_gme.filter(pl.col("z") == "zgme")["rg"][0]
    
    df = (
        df.lazy()
        .with_columns(
            [
                df["zgp"].str.extract_all(zgme).alias("GME")
            ]
        )
        .collect()
    )
    
    gme = (
        df.lazy()
        .select(["noseqsej", "GME"])
        .with_columns(pl.col("GME"))
        .explode("GME")
        .with_columns(pl.col("GME").alias("l"))
        .drop("GME")
        .lazy()
        .collect()
    )
    
    gme = parse_pmsi_fwf(gme, "ssr", "ssrha_gme", str(annee))


    # TODO GME entre 2016 -- 2021 et après 2021
    # ssrha = df.drop('zgp')

    ssrha = {"ssrha" : ssrha, "gme" : gme}
    
    return ssrha 


