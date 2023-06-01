
import polars as pl
from pypmsi.utils import *

def irha(finess, annee : int, mois : int, path : str, typi : int = 1, tdiag : bool = False, filepath = "", n_rows = None) -> dict:
    """Découpage des RHA ; 2011 à 2023
    
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
            path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "rha"
        )

    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)

    df = parse_pmsi_fwf(df, "ssr", "rha", annee)

    if str(annee) < '2013':
        df = df.with_columns(pl.col('nbcdarr').alias('nbcsarr'))

    if typi == 1:
        # retourne la partie fixe uniquement
        rha = {"rha": df.drop(["zad"]).drop(list(filter(re.compile('^fil').match, df.columns)))}
        return rha

    patterns_rha = get_patterns(str(annee), "rha")
    zacccam_pat = patterns_rha.filter(pl.col("z") == "zal")["rg"][0]
    zacccam_cur = patterns_rha.filter(pl.col("z") == "zal")["curseur"][0]

    zd_pat = patterns_rha.filter(pl.col("z") == "zd")["rg"][0]
    zd_cur = patterns_rha.filter(pl.col("z") == "zd")["curseur"][0]

    zaccsarr_pat = patterns_rha.filter(pl.col("z") == "zcsarr")["rg"][0]
    zaccsarr_cur = patterns_rha.filter(pl.col("z") == "zcsarr")["curseur"][0]

    # Définition des curseurs zones variables
    df = df.with_columns(pl.lit(0).alias("ldas_s"))
    df = df.with_columns(pl.Series(name="ldas_e", values=df["nbda"] * zd_cur))

    df = df.with_columns(
        pl.when(pl.col("nbcsarr") > 0)
        .then(pl.col("nbda") * zd_cur)
        .otherwise(pl.col("ldas_e"))
        .alias("lcsarr_s")
    )

    df = df.with_columns(
        pl.when(pl.col("nbcsarr") > 0)
        .then(
            pl.col("nbda") * zd_cur
            + pl.col("nbcsarr") * zaccsarr_cur
        )
        .otherwise(pl.col("ldas_e"))
        .alias("lcsarr_e")
    )

    df = df.with_columns(
        pl.when(pl.col("nbccam") > 0)
        .then(pl.col("nbda") * zd_cur + pl.col("nbcsarr") * zaccsarr_cur)
        .otherwise(pl.col("lcsarr_e"))
        .alias("lccam_s")
    )

    df = df.with_columns(
        pl.when(pl.col("nbccam") > 0)
        .then(
            pl.col("nbda") * zd_cur
            + pl.col("nbcsarr") * zaccsarr_cur
            + pl.col("nbccam") * zacccam_cur
        )
        .otherwise(pl.col("lcsarr_e"))
        .alias("lccam_e")
    )

    # Découper les 3 zones variables, da, csarr et ccam
    df = (
        df.lazy()
        .with_columns(
            [
                pl.struct(["zad", "ldas_e"])
                .apply(lambda x: x["zad"][slice(x["ldas_e"])])
                .alias("zdas"),
                pl.struct(["zad", "lcsarr_e", "lcsarr_s"])
                .apply(lambda x: x["zad"][slice(x["lcsarr_s"], x["lcsarr_e"])])
                .alias("zcsarr"),
                pl.struct(["zad", "lccam_e", "lccam_s"])
                .apply(lambda x: x["zad"][slice(x["lccam_s"], x["lccam_e"])])
                .alias("zccam"),
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
                    df["zccam"]
                    .str.extract_all("[A-Z]{4}[0-9]{3}")
                    .apply(lambda x: str(", ".join(set(x))))
                    .alias("stream_ccam"),
                    df["zdas"]
                    .str.extract_all("[A-Z0-9\+]{1,8}")
                    .apply(lambda x: str(", ".join(set(x))))
                    .alias("stream_das"),
                    df["zcsarr"]
                    .str.extract_all("[a-zA-Z]{2,3}[0-9\+]{2,4}")
                    .apply(lambda x: str(", ".join(set(x))))
                    .alias("stream_csarr"),
                ]
            )
            .collect()
        )

        
    if typi == 2:
        # retourne partie fixe + stream
        rha = {
            "rha": df.drop(["zad", "zccam", "zdas", "zcsarr"]).drop(list(filter(re.compile('^fil').match, df.columns)))
        }
        return rha


    # de ces zones extraires le découpage des parties variables (regexp)
    df = (
        df.lazy()
        .with_columns(
            [
                df["zdas"].str.extract_all(zd_pat).alias("das"),
                df["zcsarr"].str.extract_all(zaccsarr_pat).alias("CSARR"),
                df["zccam"].str.extract_all(zacccam_pat).alias("CCAM"),
            ]
        )
        .collect()
    )


    # consolider (explode) et parser la zone actes
    csarr = (
        df.lazy()
        .select(["noseqsej", "noseqrhs", "CSARR"])
        .with_columns(pl.col("CSARR"))
        .explode("CSARR")
        .filter(~pl.col("CSARR").is_null())
        .with_columns(pl.col("CSARR").alias("l"))
        .drop("CSARR")
        .lazy()
        .collect()
    )


    csarr = parse_pmsi_fwf(csarr, "ssr", "rha_csarr", str(annee))


    # consolider (explode) et parser la zone actes
    ccam = (
        df.lazy()
        .select(["noseqsej", "noseqrhs", "CCAM"])
        .with_columns(pl.col("CCAM"))
        .explode("CCAM")
        .filter(~pl.col("CCAM").is_null())
        .with_columns(pl.col("CCAM").alias("l"))
        .drop("CCAM")
        .lazy()
        .collect()
    )

    ccam = parse_pmsi_fwf(ccam, "ssr", "rha_ccam", str(annee))

    # consolider (explode) la zone das
    das = (
        df.lazy()
        .select(["noseqsej", "noseqrhs", "das"])
        .explode("das")
        .with_columns(pl.col("das").str.strip())
        .filter(~pl.col("das").is_null())
        .collect()
    )

    # à ce stade on drop les colonnes devenues superflues
    df = df.drop(
        [
            "zad",
            "lccam_s",
            "lccam_e",
            "lcsarr_s",
            "lcsarr_e",
            "ldas_e",
            "ldas_s",
            "CCAM",
            "CSARR",
            "das",
            "zdas",
            "zccam",
            "zcsarr",
        ]
    ).drop(list(filter(re.compile('^fil').match, df.columns)))

    # résultat sous forme d'un dictionnaire
    rha = {"rha": df, "ccam": ccam, "das": das, "csarr": csarr}

    return rha


