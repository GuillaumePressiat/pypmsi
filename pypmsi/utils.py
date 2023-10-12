import polars as pl
import re
import os


def get_formats_path() -> str:
    """Récupère le chemin du fichier pmeasyr_formats.parquet"""

    folder = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(folder, "formats", "pmeasyr_formats.parquet")
    return path


PMEASYR_FORMATS_FILE = get_formats_path()
#PMEASYR_FORMATS_FILE = 'formats/pmeasyr_formats.json'

def parse_dates(df: pl.DataFrame, patterns: str = "(dt|dat|d8).*") -> pl.DataFrame:
    """Mettre les dates au format %d%m%Y au format date

    Args:
        df (pl.DataFrame): Dataframe à parser
        patterns (str, optional): Pattern regex des noms des colonnes date à parser

    Returns:
        pl.DataFrame: Dataframe avec les dates au format date
    """
    re_dt = re.compile(patterns)

    df = df.with_columns(
        [
            pl.col(i).str.strptime(pl.Date, format="%d%m%Y", strict=False).alias(i)
            for i in list(filter(re_dt.match, df.columns))
        ]
    )
    return df


def parse_integers(df: pl.DataFrame, columns_l: list) -> pl.DataFrame:
    """Mettre certaines colonnes au format integer

    Args:
        df (pl.DataFrame): Dataframe à parser
        columns_l (str): Liste contenant les noms des colonnes à parser en integer

    Returns:
        pl.DataFrame: Dataframe avec les entiers formatés
    """

    df = df.with_columns(
        [pl.col(i).cast(pl.Int32, strict=False).alias(i) for i in columns_l]
    )
    return df

def parse_integers_regex(df: pl.DataFrame, patterns: str) -> pl.DataFrame:
    """Mettre certaines colonnes via regexau format integer

    Args:
        df (pl.DataFrame): Dataframe à parser
        patterns (str): Patterns des noms des colonnes à parser en integer

    Returns:
        pl.DataFrame: Dataframe avec les entiers formatés
    """
    re_dt = re.compile(patterns)
    df = df.with_columns(
        [pl.col(i).cast(pl.Int32, strict=False).alias(i) for i in list(filter(re_dt.match, df.columns))]
    )
    return df


def parse_numerics(df: pl.DataFrame, patterns: str, digits = 2) -> pl.DataFrame:
    """Mettre certaines colonnes au format nombre

    Args:
        df (pl.DataFrame): Dataframe à parser
        patterns (str): Patterns des noms des colonnes à parser en integer
        
    Returns:
        pl.DataFrame: Dataframe avec les nombres formatés
    """
    
    re_dt = re.compile(patterns)
    
    df = df.with_columns(
        [(pl.col(i).cast(pl.Int32, strict=False) / 10**digits).alias(i) for i in list(filter(re_dt.match, df.columns))]
    )
    return df

# Charger les formats pmeasyr (formats ministériels)
def get_formats(annee: str, champ: str, table: str) -> pl.DataFrame:
    """Charger les formats PMSI ministériels pour une année, une table d'un champ

    Args:
        annee (str): Année de la période PMSI (2 digits)
        champ (str): Champ PMSI (mco, ssr, psy, had)
        table (str): Table considérée (exemple: rsa, rss, rsa_ano, rsa_actes, rsa_um)

    Returned:
        pl.DataFrame: Formats PMSI ministériels
    """
    #formats = pl.read_json(PMEASYR_FORMATS_FILE)
    formats = pl.read_parquet(PMEASYR_FORMATS_FILE)
    

    formats_temp = (
        formats.filter(pl.col("champ") == champ)
        .filter(pl.col("table") == table)
        .with_columns(pl.col("nom").str.to_lowercase().alias("nom"))
        .with_columns(pl.col("longueur").cast(pl.Int32).alias("longueur"))
    )

    if table != 'tra':
        formats_temp = (
            formats_temp
            .filter(pl.col("an") == annee)
        )


    if ((annee > "12") & (annee < "20") & (champ == "mco") & (table == "rsa_um")):
        formats_temp = formats_temp.with_columns(
            pl.when(pl.col("nom") == "nseqrum")
            .then(pl.lit(5))
            .otherwise(pl.col("longueur"))
            .alias("longueur")
        )

    return formats_temp


# Charger les formats pmeasyr (expressions régulières et curseurs)
def get_patterns(annee4: str, table: str) -> pl.DataFrame:
    """Charger les patterns des zones variables

    Args:
        annee4 (int): Année de la période PMSI (4 caractères)
        table (str): Table considérée

    Returns:
        pl.DataFrame: Pattern regex et curseurs
    """
    #formats = pl.read_json(PMEASYR_FORMATS_FILE)
    formats = pl.read_parquet(PMEASYR_FORMATS_FILE)

    formats_temp = formats.filter(pl.col("table") == table).filter(
        pl.col("an").str.slice(0,4) == annee4
    )
    return formats_temp


def parse_pmsi_fwf(
    df: pl.DataFrame, champ: str, table: str, annee: str) -> pl.DataFrame:
    """Découpage d'un fichier partie fixe préalablement chargé dans un pl.DataFrame (colonne l)

    Args:
        df (pl.DataFrame): DataFrame à parser
        champ (str): Champ du format du df
        table (str): Table du format du df
        annee (int): Année de la période PMSI (4 digits)

    Returns:
        pl.DataFrame: Dataframe découpé
    """
    

    formats = get_formats(str(annee)[2:], champ, table)
    #return formats

    column_names = formats["nom"].to_list()
    widths = (
        formats.filter(~pl.col("longueur").is_null())["longueur"].to_list()
    )
    columns_i = formats.filter(pl.col("type") == "i")["nom"].to_list()

    if table in ["rum", "rsa", "rhs", "rha", "rps", "rpsa", "raa", "r3a", "ssrha"]:
        widths.append(int(1e9))

    slice_tuples = []
    offset = 0

    for i in widths:
        slice_tuples.append((offset, i))
        offset += i

    df = (df
          .lazy()
          .with_columns(
              [
                  pl.col("l").str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col)
                  for slice_tuple, col in zip(slice_tuples, column_names)
              ]
          )
          .drop("l")
          .collect()
         )

    df = parse_dates(df)
    df = parse_integers(df, columns_i)

    return df


def parse_pmsi_trsf(
    df: pl.DataFrame, champ: str, table: str, annee: int, typer : str
) -> pl.DataFrame:
    """Découpage d'un fichier partie fixe préalablement chargé dans un pl.DataFrame (colonne l)

    Args:
        df (pl.DataFrame): DataFrame à parser
        champ (str): Champ du format du df
        table (str): Table du format du df
        annee (int): Année de la période PMSI (4 digits)

    Returns:
        pl.DataFrame: Dataframe découpé
    """
    formats = get_formats(str(annee)[2:4], champ, table)
    formats = formats.filter(pl.col('Typer') == typer)

    column_names = formats["nom"].to_list()
    widths = (
        formats.filter(~pl.col("longueur").is_null())["longueur"].to_list()
    )
    columns_i = formats.filter(pl.col("type") == "i")["nom"].to_list()

    slice_tuples = []
    offset = 0

    for i in widths:
        slice_tuples.append((offset, i))
        offset += i

    df = (df
          .lazy()
          .with_columns(
              [
                  pl.col("l").str.slice(slice_tuple[0], slice_tuple[1]).str.strip_chars().alias(col)
                  for slice_tuple, col in zip(slice_tuples, column_names)
              ]
          )
          .drop("l")
          .collect()
         )

    df = parse_dates(df)
    df = parse_integers(df, columns_i)


    return df



def polars_to_pandas(df_d):
    """Convertir une sortie pymeasy polars en pandas
    
    Args:
        df_d (pl.DataFrame ou dict de pl.DataFrame): Sortie d'une fonction i* pymeasy
    
    Returns:
        TYPE: pd.DataFrame ou dict de pd.DataFrame
    """
    if (type(df_d) == pl.DataFrame):
        return df_d.to_pandas()
    elif (type(df_d) == dict):
        return {k: polars_to_pandas(v) for k, v in df_d.items()}

# def noyau_pmsi(finess, annee : int, mois : int, path : str, **kwargs):
# 
#     noyau = {'finess' : finess, 'annee' : annee, 'mois' : mois, 'path' : path}
#     for k,v in kwargs.items():
#         noyau[k] = v
#     return noyau

# def fichier_pmsi(p : dict, extension = "rsa"):
#     f = str(p['finess']) + "." + str(p['annee']) + "." + str(p['mois']) + "." + extension
#     return f

# def map_arguments(arguments):
#     
#     d = arguments.items()
#     # for key, value in d:
#     #     print(f"{key}: {value}")
#     
#     if ('filepath' in arguments):
#         arguments['situation'] = 'fichier'
# 
#     if (('finess' in arguments) & 
#         ('annee' in arguments) &
#         ('mois' in arguments) & 
#         ('path' in arguments)):
#         arguments['situation'] = 'noyau_pmsi'
#         arguments['filepath'] = arguments['path'] + '/' + str(arguments['finess']) + "." + str(arguments['annee']) + "." + str(arguments['mois']) + "." 
# 
#     return arguments

