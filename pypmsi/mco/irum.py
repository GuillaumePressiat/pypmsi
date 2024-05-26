
import polars as pl
from pypmsi.utils import *
import re
from pypmsi.mco.rum_particuliers import rum_particuliers



# fonction de lecture des RSA entre 2012 et 2024

def irum(
    finess, annee: int, mois: int, path: str, typi: int = 3, tdiag: bool = True, filepath = "", n_rows = None
) -> dict:
    """Découpage des RUM / RSS au format ministériel

    Args:
        finess (TYPE): Description
        annee (int): Description
        mois (int): Description
        path (str): Description
        typi (int, optional): Description
        tdiag (bool, optional): Description

    Returns:
        dict: Dictionnaire contenant des dataframe
    """

    # ok entre 2012 et 2023 

    # 4 types d'imports (typi)
    # # 1          : partie fixe uniquement
    # # 2          : partie fixe + zones streams actes, das, dad
    # # 3 (défaut) : partie fixe + partie variable
    # # 4          : partie fixe + partie variable + zones streams actes, das, dad

    # Transposition des diagnostics (tdiag)
    # # True : renvoie une seule table avec les diags par position, 1 dp, 2 dr, 3 das, 4 dad
    # # False : renvoie deux tables séparés avec les das et les dad

    if filepath != "":
        file_in = filepath
    else:
        file_in = (
            path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "rss.txt"
        )
        
    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)

    df = parse_pmsi_fwf(df, "mco", "rum", str(annee))
    

    # calcul sur les dates (difftime)
    df = df.with_columns(
        (pl.col("d8soue") - pl.col("d8eeue")).dt.days().alias("dureesejpart")
    )

    if typi == 1:
    # retourne la partie fixe uniquement
        rum = {"rum": df.drop(["zad"]).drop(list(filter(re.compile('^fil').match, df.columns)))}
        return rum

    if (annee in [2015]):
        return rum_particuliers(df, annee, typi, tdiag)


    patterns_rum = get_patterns(str(annee), "rum")
    zac_pat = patterns_rum.filter(pl.col("z") == "zac")["rg"][0]
    zac_cur = patterns_rum.filter(pl.col("z") == "zac")["curseur"][0]

    zd_pat = patterns_rum.filter(pl.col("z") == "zd")["rg"][0]
    zd_cur = patterns_rum.filter(pl.col("z") == "zd")["curseur"][0]

    zdad_pat = patterns_rum.filter(pl.col("z") == "zdad")["rg"][0]
    zdad_cur = patterns_rum.filter(pl.col("z") == "zdad")["curseur"][0]

    zal_pat = patterns_rum.filter(pl.col("z") == "zal")["rg"][0]
    zal_cur = patterns_rum.filter(pl.col("z") == "zal")["curseur"][0]



    # Définition des curseurs zones variables
    df = df.with_columns(pl.lit(0).alias("ldas_s"))
    df = df.with_columns(pl.Series(name="ldas_e", values=df["nbdas"] * zd_cur))

    df = df.with_columns(
        pl.when(pl.col("nbdad") > 0)
        .then(pl.col("nbdas") * zd_cur)
        .otherwise(pl.col("ldas_e"))
        .alias("ldad_s")
    )

    df = df.with_columns(
        pl.when(pl.col("nbdad") > 0)
        .then(pl.col("nbdas") * zd_cur + pl.col("nbdad") * zdad_cur)
        .otherwise(pl.col("ldas_e"))
        .alias("ldad_e")
    )

    df = df.with_columns(
        pl.when(pl.col("nbacte") > 0)
        .then(pl.col("nbdas") * zd_cur + pl.col("nbdad") * zdad_cur)
        .otherwise(pl.col("ldad_e"))
        .alias("lacte_s")
    )

    df = df.with_columns(
        pl.when(pl.col("nbacte") > 0)
        .then(
            pl.col("nbdas") * zd_cur
            + pl.col("nbdad") * zdad_cur
            + pl.col("nbacte") * zal_cur
        )
        .otherwise(pl.col("ldad_e"))
        .alias("lacte_e")
    )

    # Découper les 3 zones variables, das, dad et actes
    df = (
        df.lazy()
        .with_columns(
            [
                pl.struct(["zad", "ldas_e"])
                .map_elements(lambda x: x["zad"][slice(x["ldas_e"])], return_dtype = pl.String)
                .alias("zdas"),
                pl.struct(["zad", "ldad_e", "ldad_s"])
                .map_elements(lambda x: x["zad"][slice(x["ldad_s"], x["ldad_e"])], return_dtype = pl.String)
                .alias("zdad"),
                pl.struct(["zad", "lacte_e", "lacte_s"])
                .map_elements(lambda x: x["zad"][slice(x["lacte_s"], x["lacte_e"])], return_dtype = pl.String)
                .alias("zactes"),
            ]
        )
        .collect()
    )

    # de ces zones extraires le découpage des parties variables (regexp)
    df = (
        df.lazy()
        .with_columns(
            [
                df["zdas"].str.extract_all(zd_pat).alias("das"),
                df["zdad"].str.extract_all(zdad_pat).alias("dad"),
                df["zactes"].str.extract_all(zal_pat).alias("ACTES"),
            ]
        )
        .collect()
    )

    if typi in [2, 4]:
        # extraction des zones stream
        df = (
            df.lazy()
            .with_columns(
                [
                    df["zactes"]
                    .str.extract_all("[A-Z]{4}[0-9]{3}")
                    .map_elements(lambda x: str(", ".join(set(x))), return_dtype = pl.String)
                    .alias("stream_actes"),
                    df["zdas"]
                    .str.extract_all("[A-Z0-9\+]{1,8}")
                    .map_elements(lambda x: str(", ".join(set(x))), return_dtype = pl.String)
                    .alias("stream_das"),
                    df["zdad"]
                    .str.extract_all("[a-zA-Z0-9\+]{1,8}")
                    .map_elements(lambda x: str(", ".join(set(x))), return_dtype = pl.String)
                    .alias("stream_dad"),
                ]
            )
            .collect()
        )

    if typi == 2:
        # retourne partie fixe + stream
        rum = {
            "rum": df.drop([
                    "ldad_s",
                    "zad",
                    "ldad_e",
                    "ldad_s",
                    "das",
                    "dad",
                    "lacte_s",
                    "lacte_e",
                    "ldas_e",
                    "zactes", "zdas", "zdad", "ACTES"]).drop(list(filter(re.compile('^fil').match, df.columns)))
        }
        return rum

    # consolider (explode) et parser la zone actes
    actes = (
        df.lazy()
        .select(["norss", "norum", "nas", "ACTES"])
        .with_columns(pl.col("ACTES"))
        .explode("ACTES")
        .filter(~pl.col("ACTES").is_null())
        .with_columns(pl.col("ACTES").alias("l"))
        .drop("ACTES")
        .lazy()
        .collect()
    )

    actes = parse_pmsi_fwf(actes, "mco", "rum_actes", str(annee))

    # consolider (explode) la zone das
    das = (
        df.lazy()
        .select(["norss", "norum", "nas", "das"])
        .explode("das")
        .with_columns(pl.col("das").str.strip_chars())
        .filter(~pl.col("das").is_null())
        .collect()
    )

    # consolider (explode) la zone dad
    dad = (
        df.lazy()
        .select(["norss", "norum", "nas", "dad"])
        .explode("dad")
        .filter(~pl.col("dad").is_null())
        .with_columns(pl.col("dad").str.strip_chars())
        .collect()
    )

    # à ce stade on drop les colonnes devenues superflues
    df = df.drop(
        [
            "ldad_s",
            "zad",
            "ldad_e",
            "lacte_s",
            "lacte_e",
            "ldas_e",
            "ldas_s",
            "ACTES",
            "dad",
            "das",
            "zdad",
            "zactes",
            "zdas",
        ]
    ).drop(list(filter(re.compile('^fil').match, df.columns)))

    # résultat sous forme d'un dictionnaire
    rum = {"rum": df, "actes": actes, "dad": dad, "das": das}

    # si tdiag alors on transpose les diags
    if tdiag == False:
        return rum
    else:
        rum_dp = (
            rum["rum"]
            .select(["norum", "norss", "nas", "dp"])
            .with_columns((pl.lit(1).alias("position")))
            .rename({"dp": "diag"})
        )
        rum_dr = (
            rum["rum"]
            .select(["norum", "norss", "nas", "dr"])
            .with_columns((pl.lit(2).alias("position")))
            .rename({"dr": "diag"})
        )
        rum_das = (
            rum["das"]
            .select(["norum", "norss", "nas", "das"])
            .with_columns((pl.lit(3).alias("position")))
            .rename({"das": "diag"})
        )
        rum_dad = (
            rum["dad"]
            .select(["norum", "norss", "nas", "dad"])
            .with_columns((pl.lit(4).alias("position")))
            .rename({"dad": "diag"})
        )
        rum_diags = pl.concat([rum_dp, rum_dr, rum_das, rum_dad]).filter(
            pl.col("diag") != ""
        )
        rum = {"rum": df, "actes": actes, "diags": rum_diags}

    return rum

