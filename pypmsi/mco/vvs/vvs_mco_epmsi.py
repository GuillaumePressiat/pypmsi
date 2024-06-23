
import polars as pl
import polars.selectors as cs
from pypmsi.mco.vvs.vvs_mco_libelles_valo import vvs_mco_libelles_valo

def vvs_mco_epmsi_sv(vvs_df):

    return (
    vvs_df
    .group_by('type_fin')
    .agg(pl.col('cle_rsa').count().alias('nb_rsa'),
            pl.col('rec_totale').sum().round(2).alias('recette_br_avec_coeffs'))
    .sort('type_fin')
    .join(vvs_mco_libelles_valo()['vvs_mco_lib_type_sej'], on = 'type_fin', how = 'left')
    )


def vvs_mco_epmsi_rav(vvs_df):

    return (
    vvs_df
    .select(cs.starts_with('rec'))
    .sum()
    .unpivot(variable_name = 'rubrique', value_name = 'recette')
    .join(vvs_mco_libelles_valo()['vvs_mco_lib_valo'], on = 'rubrique', how = 'left')
    )

