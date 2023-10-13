
import polars as pl
from pypmsi.utils import *


# tarifs = rp.get_table('tarifs_mco_ghs')

def vvs_ghs_supp(
        rsa, 
        tarifs,
        supplements = None, 
        ano = None, 
        porg = pl.DataFrame(),
        diap = pl.DataFrame(),
        pie  = pl.DataFrame(),
        mo   = pl.DataFrame(),
        full = False, 
        cgeo = 1, 
        prudent = None,
        bee = True, 
        csegur = 1.0019):

    
    n_anseqta = rsa['anseqta'].unique().shape[0]


    
    tarifs = pl.concat(
        [
        tarifs, 
        pl.DataFrame({'anseqta' : rsa['anseqta'].unique()})
        .with_columns(pl.repeat('9999', n_anseqta).alias('ghs'),
                      pl.repeat(0, n_anseqta, dtype=pl.Float64).alias('tarif_base'),
                      pl.repeat(0, n_anseqta, dtype=pl.Float64).alias('tarif_exb'),
                      pl.repeat(0, n_anseqta, dtype=pl.Float64).alias('tarif_exh'),
                      pl.repeat(0, n_anseqta, dtype=pl.Float64).alias('forfait_exb'))
        ], 
        how = 'diagonal')


    if porg.shape[0] == 0:
        porg = pl.DataFrame({'cle_rsa': '', 'cdpo' : ''})

    if pie.shape[0] == 0:
        pie = pl.DataFrame({'cle_rsa': '', 'code_pie' : 'REP', 'nbsuppie': 0})

    if diap.shape[0] == 0:
        diap = pl.DataFrame({'cle_rsa': '', 'nbsup' : 0})

    if mo.shape[0] == 0:
        mo = pl.DataFrame({'cle_rsa': '', 'cducd' : ''})


    if prudent is None:
        rsa_2 = (
            rsa
            .filter(pl.col('noghs').str.slice(0,1) != 'I')
            .with_columns(
                pl.when(pl.col('anseqta') == '2023').then(0.993 * 1.0023)
                  .when(pl.col('anseqta') == '2022').then(0.993 * 1.0013)
                  .when(pl.col('anseqta') == '2021').then(0.993 * 1.0019)
                  .when(pl.col('anseqta') == '2020').then(0.993)
                  .when(pl.col('anseqta') == '2019').then(0.993)
                  .when(pl.col('anseqta') == '2019').then(0.993)
                  .when(pl.col('anseqta') == '2018').then(0.993)
                  .when(pl.col('anseqta') == '2017').then(0.993)
                  .when(pl.col('anseqta') == '2016').then(0.995)
                  .when(pl.col('anseqta') == '2015').then(0.9965)
                  .when(pl.col('anseqta') == '2014').then(0.9965)
                  .when(pl.col('anseqta') == '2013').then(0.9965)
                  .otherwise(1).alias('cprudent')
                  )
            .join(tarifs.unique(['ghs', 'anseqta']), left_on = ['anseqta', 'noghs'], right_on = ['anseqta', 'ghs'], how = 'left')
            .with_columns(
                (pl.col('nbseance') * pl.col('tarif_base') * cgeo * pl.col('cprudent')).alias('t_base'),
                (pl.col('nbjrbs') * pl.col('tarif_exh') * cgeo * pl.col('cprudent')).alias('t_haut'),
                (
                    pl.when(pl.col('sejinfbi') == '2').then((pl.col('nbjrexb') / 10) * pl.col('tarif_exb') * cgeo * pl.col('cprudent'))
                    .when(pl.col('sejinfbi') == '1').then(pl.col('forfait_exb') * cgeo * pl.col('cprudent'))
                    .otherwise(0)
                ).alias('t_bas'))
                .with_columns((pl.col('t_base') + pl.col('t_haut') - pl.col('t_bas')).alias('rec_bee'))
                .with_columns(pl.col('rec_bee').alias('rec_totale'))
            )

    return rsa_2
