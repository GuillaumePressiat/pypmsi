
import polars as pl
from pypmsi.utils import *

def irsa(finess, annee : int, mois : int, path : str, typi : int = 1, tdiag : bool = True, filepath = "", n_rows = None) -> dict:
    """Découpage des RSA ; 2011 à 2023
    
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
    # ok depuis 2016 pour le moment

    # 6 types d'imports (typi)
    # # 1          : partie fixe uniquement
    # # 2          : partie fixe + zones streams actes, das
    # # 3          : partie fixe + zones streams actes, das, dpum, drum, typaut
    # # 3 (défaut) : partie fixe + partie variable
    # # 4          : partie fixe + partie variable + zones streams actes, das
    # # 5          : partie fixe + partie variable + zones streams actes, das, dpum, drum, typaut
    # # 6          : partie fixe + partie variable + zones streams actes, das, dpum, drum, typaut

    # Transposition des diagnostics (tdiag)
    # # True : renvoie une seule table avec les diags par position, 1 dp, 2 dr, 3 das, 4 dad
    # # False : renvoie deux tables séparés avec les das et les dad
    
    if filepath != "":
        file_in = filepath
    else:
        file_in = (
            path + "/" + str(finess) + "." + str(annee) + "." + str(mois) + "." + "rsa"
        )

    df = pl.read_csv(file_in, has_header=False, skip_rows=0, new_columns=["l"], n_rows = n_rows)

    df = parse_pmsi_fwf(df, "mco", "rsa", annee)

    patterns_rsa = get_patterns(str(annee), "rsa")
    zac_pat = patterns_rsa.filter(pl.col("z") == "zac")["rg"][0]
    zac_cur = patterns_rsa.filter(pl.col("z") == "zac")["curseur"][0]

    zd_pat = patterns_rsa.filter(pl.col("z") == "zd")["rg"][0]
    zd_cur = patterns_rsa.filter(pl.col("z") == "zd")["curseur"][0]

    zum_pat = patterns_rsa.filter(pl.col("z") == "zum")["rg"][0]
    zum_cur = patterns_rsa.filter(pl.col("z") == "zum")["curseur"][0]

    zal_pat = patterns_rsa.filter(pl.col("z") == "zal")["rg"][0]
    zal_cur = patterns_rsa.filter(pl.col("z") == "zal")["curseur"][0]

    # Définition des curseurs zones variables
    if str(annee) <= "2011":
        df = df.with_columns(pl.lit(0).alias("nbautpgv"))

    #     # définir les integer
    #     df = df.with_columns(pl.Series(name = 'ndas', values = df['ndas'].cast(pl.Int64)))
    #     df = df.with_columns(pl.Series(name = 'nbrum', values = df['nbrum'].cast(pl.Int64)))
    #     df = df.with_columns(pl.Series(name = 'nbautpgv', values = df['nbautpgv'].cast(pl.Int64)))
    #     df = df.with_columns(pl.Series(name = 'nb_rdth', values = df['nb_rdth'].cast(pl.Int64)))
    #     df = df.with_columns(pl.Series(name = 'na', values = df['na'].cast(pl.Int64)))

    if typi == 1:
        # retourne la partie fixe uniquement
        rsa = {"rsa": df.drop(["za"]).drop(list(filter(re.compile('^fil').match, df.columns)))}
        return rsa

    df = df.with_columns(pl.lit(0).alias("aut_s"))
    df = df.with_columns(
        pl.when(pl.col("nbautpgv") > 0)
        .then(pl.col("nbautpgv") * 2)
        .otherwise(pl.col("aut_s"))
        .alias("aut_e")
    )
    df = df.with_columns(pl.col("aut_e").alias("rdth_s"))
    df = df.with_columns(
        pl.when(pl.col("nb_rdth") > 0)
        .then(pl.col("aut_e") + pl.col("nb_rdth") * 7)
        .otherwise(pl.col("rdth_s"))
        .alias("rdth_e")
    )
    df = df.with_columns(pl.col("rdth_e").alias("rum_s"))
    df = df.with_columns(
        pl.when(pl.col("nbrum") > 0)
        .then(pl.col("rum_s") + pl.col("nbrum") * zum_cur)
        .otherwise(pl.col("rum_s"))
        .alias("rum_e")
    )
    df = df.with_columns(pl.col("rum_e").alias("das_s"))
    df = df.with_columns(
        pl.when(pl.col("ndas") > 0)
        .then(pl.col("das_s") + pl.col("ndas") * zd_cur)
        .otherwise(pl.col("das_s"))
        .alias("das_e")
    )
    df = df.with_columns(pl.col("das_e").alias("actes_s"))
    df = df.with_columns(
        pl.when(pl.col("na") > 0)
        .then(pl.col("actes_s") + pl.col("na") * zal_cur)
        .otherwise(pl.col("actes_s"))
        .alias("actes_e")
    )

    df = (
        df.lazy()
        .with_columns(
            [
                pl.struct(["za", "aut_e"])
                .apply(lambda x: x["za"][slice(x["aut_e"])])
                .alias("zaut"),
                pl.struct(["za", "rdth_s", "rdth_e"])
                .apply(lambda x: x["za"][slice(x["rdth_s"], x["rdth_e"])])
                .alias("zrdth"),
                pl.struct(["za", "rum_e", "rum_s"])
                .apply(lambda x: x["za"][slice(x["rum_s"], x["rum_e"])])
                .alias("zum"),
                pl.struct(["za", "das_s", "das_e"])
                .apply(lambda x: x["za"][slice(x["das_s"], x["das_e"])])
                .alias("zdas"),
                pl.struct(["za", "actes_s", "actes_e"])
                .apply(lambda x: x["za"][slice(x["actes_s"], x["actes_e"])])
                .alias("zactes"),
            ]
        )
        .collect()
    )

    # Découper les 3 zones variables, das, dad et actes

    # de ces zones extraires le découpage des parties variables (regexp)
    df = (
        df.lazy()
        .with_columns(
            [
                df["zdas"].str.extract_all(zd_pat).alias("das"),
                df["zum"].str.extract_all(zum_pat).alias("UM"),
                df["zactes"].str.extract_all(zal_pat).alias("ACTES"),
            ]
        )
        .collect()
    )

    if typi in [2, 3, 5, 6]:
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
                    .str.extract_all("[A-Z][0-9\+]{1,8}")
                    .apply(lambda x: str(", ".join(set(x))))
                    .alias("stream_das"),
                ]
            )
            .collect()
        )

    if typi in [3, 6]:
        # on définit les curseurs pour les zones dpum, drum
        curseurs_diag_um = (get_formats(str(annee)[2:],"mco", "rsa_um")
                            .filter(pl.col('nom').str.contains('d(p|r)um'))
                            .with_columns((pl.col('position') - 1).alias('diag_um_d'),
                                          (pl.col('position') + 4).alias('diag_um_f'))
                            .select('nom', 'diag_um_d', 'diag_um_f')
                           )

        sdpum = curseurs_diag_um.filter(pl.col('nom') == 'dpum').select('diag_um_d').to_series().to_list()[0]
        edpum = curseurs_diag_um.filter(pl.col('nom') == 'dpum').select('diag_um_f').to_series().to_list()[0]
        sdrum = curseurs_diag_um.filter(pl.col('nom') == 'drum').select('diag_um_d').to_series().to_list()[0]
        edrum = curseurs_diag_um.filter(pl.col('nom') == 'drum').select('diag_um_f').to_series().to_list()[0]

        df = (
            df.lazy()
            .with_columns(
                [
                    pl.col("zum")
                    .str.extract_all("[0-9]{2}[AB ][PCM]")
                    .apply(lambda x: str(", ".join(x)))
                    .alias("stream_um"),
                    pl.col("UM")
                    .apply(
                        lambda x: str(
                            ", ".join(set(x.apply(lambda y: y[slice(sdpum, edpum)].rstrip())))
                        )
                    )
                    .alias("stream_dpum"),
                    pl.col("UM")
                    .apply(
                        lambda x: str(
                            ", ".join(set(x.apply(lambda y: y[slice(sdrum, edrum)])))
                        )
                    )
                    .str.strip_chars()
                    .alias("stream_drum"),
                ]
            )
            .collect()
        )

    if typi in [2, 3]:
        # retourne partie fixe + stream
        rsa = {
            "rsa": df.drop(
                [
                    "za",
                    "ACTES",
                    "UM",
                    "das",
                    "aut_s",
                    "aut_e",
                    "rdth_s",
                    "rdth_e",
                    "rum_s",
                    "rum_e",
                    "das_s",
                    "das_e",
                    "actes_s",
                    "actes_e",
                    "zaut",
                    "zrdth",
                    "zum",
                    "zdas",
                    "zactes",
                ]
            ).drop(list(filter(re.compile('^fil').match, df.columns)))
        }
        return rsa

    # consolider (explode)
    rsa_um = (
        df.lazy()
        .select(["cle_rsa", "UM"])
        .explode("UM")
        .filter(~pl.col("UM").is_null())
        .with_columns(pl.col("UM").str.slice(0, 2).alias("nseqrum"))
        .with_columns(pl.col("UM").alias("l"))
        .drop("UM")
        .collect()
    )

    if str(annee) <= "2012":
        rsa_um = (
            rsa_um.with_columns(pl.lit(1).alias("id"))
            .drop("nseqrum")
            .select(
                [
                    pl.all().exclude("id"),
                    pl.col("id")
                    .cumsum()
                    .over("cle_rsa")
                    .cast(pl.Utf8)
                    .str.rjust(2, "0")
                    .alias("nseqrum"),
                ]
            )
        )

    rsa_um = parse_pmsi_fwf(rsa_um, "mco", "rsa_um", annee)

    # consolider (explode) et parser la zone actes
    actes = (
        df.lazy()
        .select(["cle_rsa", "ACTES"])
        .with_columns(pl.col("ACTES"))
        .explode("ACTES")
        .filter(~pl.col("ACTES").is_null())
        .with_columns(pl.col("ACTES").alias("l"))
        .drop("ACTES")
        .lazy()
        .collect()
    )

    actes = parse_pmsi_fwf(actes, "mco", "rsa_actes", annee)

    actes_pivot_rum = (
        rsa_um.select(["nseqrum", "nbacte"])
        .filter(pl.col("nbacte") > 0)
        .filter(~pl.col("nbacte").is_null())
        .select(pl.exclude("nbacte").repeat_by("nbacte").explode())
    )

    # consolider (explode) la zone das
    das = (
        df.lazy()
        .select(["cle_rsa", "das"])
        .explode("das")
        .with_columns(pl.col("das").str.strip_chars())
        .filter(~pl.col("das").is_null())
        .collect()
    )

    das_pivot_rum = (
        rsa_um.select(["nseqrum", "nbdiagas"])
        .filter(pl.col("nbdiagas") > 0)
        .select(pl.exclude("nbdiagas").repeat_by("nbdiagas").explode())
    )

    das = pl.concat([das, das_pivot_rum], how="horizontal")
    actes = pl.concat([actes, actes_pivot_rum], how="horizontal")

    df = df.drop(
        [
            "ACTES",
            "UM",
            "das",
            "aut_s",
            "aut_e",
            "rdth_s",
            "rdth_e",
            "rum_s",
            "rum_e",
            "das_s",
            "das_e",
            "actes_s",
            "actes_e",
            "zaut",
            "zrdth",
            "zum",
            "zdas",
            "zactes",
            "za",
        ]
    ).drop(list(filter(re.compile('^fil').match, df.columns)))

    rsa = {"rsa": df, "actes": actes, "das": das, "rsa_um": rsa_um}

    # si tdiag alors on transpose les diags
    if tdiag == False:
        return rsa
    else:
        rsa_dp = (
            rsa["rsa"]
            .select(["cle_rsa", "noseqrum", "dp"])
            .with_columns((pl.lit(1).alias("position")))
            .rename({"dp": "diag", "noseqrum": "nseqrum"})
        )
        rsa_dr = (
            rsa["rsa"]
            .select(["cle_rsa", "noseqrum", "dr"])
            .with_columns((pl.lit(2).alias("position")))
            .rename({"dr": "diag", "noseqrum": "nseqrum"})
        )
        rsa_um_dp = (
            rsa["rsa_um"]
            .select(["cle_rsa", "nseqrum", "dpum"])
            .with_columns((pl.lit(3).alias("position")))
            .rename({"dpum": "diag"})
        )
        rsa_um_dr = (
            rsa["rsa_um"]
            .select(["cle_rsa", "nseqrum", "drum"])
            .with_columns((pl.lit(4).alias("position")))
            .rename({"drum": "diag"})
        )
        rsa_um_das = (
            rsa["das"]
            .select(["cle_rsa", "nseqrum", "das"])
            .with_columns((pl.lit(5).alias("position")))
            .rename({"das": "diag"})
        )
        rsa_diags = pl.concat([rsa_dp, rsa_dr, rsa_um_dp, rsa_um_dr, rsa_um_das]).filter(
            pl.col("diag") != ""
        )
        rsa = {"rsa": df, "actes": actes, "diags": rsa_diags, "rsa_um": rsa_um}

    return rsa
