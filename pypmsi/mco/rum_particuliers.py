import polars as pl
from pypmsi.utils import *
import re

def rum_particuliers(df, annee, typi, tdiag):
    
    patterns_rum = get_patterns(str(annee), "rum")
    zac_pat = patterns_rum.filter(pl.col("z") == "zac")["rg"][0]
    zac_cur = patterns_rum.filter(pl.col("z") == "zac")["curseur"][0]

    zd_pat = patterns_rum.filter(pl.col("z") == "zd")["rg"][0]
    zd_cur = patterns_rum.filter(pl.col("z") == "zd")["curseur"][0]

    zdad_pat = patterns_rum.filter(pl.col("z") == "zdad")["rg"][0]
    zdad_cur = patterns_rum.filter(pl.col("z") == "zdad")["curseur"][0]
    levan = patterns_rum['an'].unique().to_list()
    vers  = patterns_rum['an'].str.slice(5,9).unique().to_list()

    zal_pat1 = patterns_rum.filter(pl.col("z") == "zal").filter(pl.col('an').str.slice(5,9) == vers[0])["rg"][0]
    zal_cur1 = patterns_rum.filter(pl.col("z") == "zal").filter(pl.col('an').str.slice(5,9) == vers[0])["curseur"][0]
    zal_pat2 = patterns_rum.filter(pl.col("z") == "zal").filter(pl.col('an').str.slice(5,9) == vers[1])["rg"][0]
    zal_cur2 = patterns_rum.filter(pl.col("z") == "zal").filter(pl.col('an').str.slice(5,9) == vers[1])["curseur"][0]


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
        .then(pl.when(pl.col('noverg') == vers[0]).then(
            pl.col("nbdas") * zd_cur
            + pl.col("nbdad") * zdad_cur
            + pl.col("nbacte") * zal_cur1
        ).otherwise(pl.col("nbdas") * zd_cur
            + pl.col("nbdad") * zdad_cur
            + pl.col("nbacte") * zal_cur2))
        .otherwise(pl.col("ldad_e"))
        .alias("lacte_e")
    )

    df = (
        df.lazy()
        .with_columns(
            [
                pl.struct(["zad", "ldas_e"])
                .apply(lambda x: x["zad"][slice(x["ldas_e"])])
                .alias("zdas"),
                pl.struct(["zad", "ldad_e", "ldad_s"])
                .apply(lambda x: x["zad"][slice(x["ldad_s"], x["ldad_e"])])
                .alias("zdad"),
                pl.struct(["zad", "lacte_e", "lacte_s"])
                .apply(lambda x: x["zad"][slice(x["lacte_s"], x["lacte_e"])])
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
                pl.when(pl.col('noverg') == vers[0]).then(pl.col('zactes').str.extract_all(zal_pat1))
                .otherwise(pl.col('zactes').str.extract_all(zal_pat2)).alias("ACTES")
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
                    .apply(lambda x: str(", ".join(set(x))))
                    .alias("stream_actes"),
                    df["zdas"]
                    .str.extract_all("[A-Z0-9\+]{1,8}")
                    .apply(lambda x: str(", ".join(set(x))))
                    .alias("stream_das"),
                    df["zdad"]
                    .str.extract_all("[a-zA-Z0-9\+]{1,8}")
                    .apply(lambda x: str(", ".join(set(x))))
                    .alias("stream_dad"),
                ]
            )
            .collect()
        )

    if typi == 2:
        # retourne partie fixe + stream
        rum = {
            "rum": df.drop(["zad", "zactes", "zdas", "zdad", "ACTES"]).drop(list(filter(re.compile('^fil').match, df.columns)))
        }
        return rum


    # consolider (explode) et parser la zone actes
    actes = (
        df.lazy()
        .select(["norss", "norum", "nas", "ACTES", 'noverg'])
        .with_columns(pl.col("ACTES"))
        .explode("ACTES")
        .filter(~pl.col("ACTES").is_null())
        .with_columns(pl.col("ACTES").alias("l"))
        .drop("ACTES")
        .lazy()
        .collect()
    )


    actes = pl.concat([parse_pmsi_fwf(actes.filter(pl.col('noverg') == vers[0]),  "mco", "rum_actes", str(annee) + "_" + vers[0]),
                       parse_pmsi_fwf(actes.filter(pl.col('noverg') == vers[1]), "mco", "rum_actes", str(annee) + "_" + vers[1])]).drop('noverg')



    # consolider (explode) la zone das
    das = (
        df.lazy()
        .select(["norss", "norum", "nas", "das"])
        .explode("das")
        .with_columns(pl.col("das").str.strip())
        .filter(~pl.col("das").is_null())
        .collect()
    )

    # consolider (explode) la zone dad
    dad = (
        df.lazy()
        .select(["norss", "norum", "nas", "dad"])
        .explode("dad")
        .filter(~pl.col("dad").is_null())
        .with_columns(pl.col("dad").str.strip())
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

