
import polars as pl
import polars.selectors as cs
from pypmsi.utils import *

def vvs_rsa(rsa):

    df = (rsa['rsa']
        .select(
            'cle_rsa', 
            'duree', 
            'rsacmd', 
            'typesej', 
            'noghs', 
            'moissor', 
            'ansor', 
            'sexe', 
            'nbjrbs', 
            'nbjrexb', 
            'sejinfbi', 
            'agean', 
            'agejr', 
            'nbseance', 
            'anseqta',
            'nbacte9615', 
            'ghm', 
            'zrdth', 
            'echpmsi', 
            'prov', 
            'dest', 
            'schpmsi',
            'nb_rdth', 
            'nbrum', 
            cs.starts_with('nbsup'),
            cs.starts_with('sup'),
            cs.starts_with('topctc')
            )
        .with_columns(pl.when(
            ((pl.col('rsacmd') == '28') & ~(pl.col('ghm').is_in(['28Z19Z', '28Z20Z', '28Z21Z', '28Z22Z'])))).then(pl.col('nbseance'))
            .when(pl.col('ghm').is_in(['28Z19Z', '28Z20Z', '28Z21Z', '28Z22Z'])).then(pl.lit(1))
            .otherwise(pl.lit(1)))
    ).join(
        (
            rsa['rsa_um']
            .filter(pl.col('typaut1').str.slice(0,2) == '07')
            .filter(pl.col('nseqrum') == '01')
            .unique('cle_rsa')
            .with_columns(pl.lit(1).alias('uhcd'))
        ),
        on = 'cle_rsa', how = 'left'
    ).with_columns(pl.when(((pl.col('uhcd') == 1) & (pl.col('nbrum') == 1))).then(pl.lit(1)).otherwise(pl.lit(0)).alias('monorum_uhcd'))


    return df


def vvs_rsa_hors_periode(vrsa, an_v, mois_v):

    vrsa_ = (vrsa
        .with_columns(
        ((pl.col('ansor') != an_v) | (pl.col('moissor') > mois_v)).alias('rsa_hors_periode')
        )
        .with_columns(
            pl.when(pl.col('rsa_hors_periode')).then(pl.lit('90Z99Z')).otherwise(pl.col('ghm')).alias('ghm'),
            pl.when(pl.col('rsa_hors_periode')).then(pl.lit('9999')).otherwise(pl.col('noghs')).alias('noghs'),
            pl.when(pl.col('rsa_hors_periode')).then(pl.lit('90')).otherwise(pl.col('rsacmd')).alias('rsacmd')
        )
        )

    return vrsa_

def vvs_ano_mco(ano):

    return ano


