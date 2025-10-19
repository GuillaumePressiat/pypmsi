
import polars as pl
from pypmsi.utils import *
import polars.selectors as cs

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
        csegur = 1.0035):

    
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



    # Correction GHS 5907 suite erreur dans tarifs atih entrainant une valo négative en cas de borne basse    
    if "2018" in rsa['anseqta'].unique():
        rsa = (
            rsa
            .with_columns(pl.when((pl.col('ghm') == '15M06A') & 
                                  (pl.col('noghs') == '5907') & 
                                  (pl.col('duree') == 0) & 
                                  (pl.col('nbjrexb') == 30) &
                                  (pl.col('anseqta') == '2018')).then(pl.col('nbjrexb') - 10).otherwise(pl.col('nbjrexb')))
            .with_columns(pl.when((pl.col('ghm') == '15M06A') & 
                                  (pl.col('noghs') == '5907') & 
                                  (pl.col('duree') == 1) & 
                                  (pl.col('monorum_uhcd') == 1) & 
                                  (pl.col('nbjrexb') == 30) &
                                  (pl.col('anseqta') == '2018')).then(pl.col('nbjrexb') - 10).otherwise(pl.col('nbjrexb')))
            )


    # Switch de GHS si molécule Yescarta ou Kymriah (car-T cells)
    cart_cells = (
        mo
        .filter(pl.col('cducd').str.slice(5,7).is_in(['9439938', '9439921']))
        .unique('cle_rsa')
        .with_columns(pl.lit(1).alias('switch_ghs'))
        )

    rsa = (
        rsa
        .join(cart_cells, on = 'cle_rsa', how = 'left')
        .with_columns(pl.when(
            (~pl.col('switch_ghs').is_null()) & (((pl.col('anseqta') == '2017') & (pl.col('ansor') == '2018')) | (pl.col('anseqta') == '2018'))
                )
            .then(pl.col('noghs')).otherwise(pl.lit(''))
        .alias('old_noghs'),
        pl.when(
            (~pl.col('switch_ghs').is_null()) & (((pl.col('anseqta') == '2017') & (pl.col('ansor') == '2018')) | (pl.col('anseqta') == '2018'))
                ).then(pl.lit('8973')).otherwise(pl.col('noghs'))
            .alias('noghs')
            )
        )

    # Calcul des coefficients en fonction de l'année séquentielle des RSA 
    # > on pourrait avantageusement le faire dans une table annexe avec une ligne par année
    if prudent is None:
        rsa = (
            rsa
            .with_columns(
                pl.when(pl.col('anseqta') == '2025').then(0.993 * 1.0035)
                  .when(pl.col('anseqta') == '2024').then(0.993 * 1.0042)
                  .when(pl.col('anseqta') == '2023').then(0.993 * 1.0023)
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
            )
    else:
        rsa = (
            rsa
            .with_columns(
                pl.when(pl.col('anseqta') == '2025').then(prudent * 1.0035)
                  .when(pl.col('anseqta') == '2024').then(prudent * 1.0042)
                  .when(pl.col('anseqta') == '2023').then(prudent * 1.0023)
                  .when(pl.col('anseqta') == '2022').then(prudent * 1.0013)
                  .when(pl.col('anseqta') == '2021').then(prudent * 1.0019)
                  .otherwise(prudent).alias('cprudent')
                  )
            )

    rsa_2 = (
        rsa
        .filter(pl.col('noghs').str.slice(0,1) != 'I')
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


    # https://sante.gouv.fr/systeme-de-sante/innovation-et-recherche/forfait-innovation
    rsa_innovation = (
        rsa
        .filter(pl.col('noghs').str.slice(0,1) == 'I')
        .with_columns(
            pl.when(pl.col('noghs') == 'I08').then(3119)
              .when(pl.col('noghs') == 'I17').then(4770)
              .otherwise(None).cast(pl.Float64).alias('t_base'))
        .with_columns(
            pl.col('t_base').alias('rec_bee'),
            pl.col('t_base').alias('rec_totale'),
            pl.lit(None, dtype = pl.Float64).alias('t_bas'),
            pl.lit(None, dtype = pl.Float64).alias('t_haut')
                )
        )

    rsa_valo = pl.concat([rsa_2, rsa_innovation], how = 'diagonal')


    if bee is True:
        return (
            rsa_valo
            .select(['cle_rsa', 'nbseance', 'rec_totale', 't_base', 't_bas', 't_haut', 'rec_bee', 'ghm', 'noghs', 'anseqta', 'moissor'])
            .rename({'t_bas' : 'rec_exb', 't_base' : 'rec_base', 't_haut' : 'rec_exh'})
            )



    # Info suppléments ghs radiothérapie
    rdth = (
        rsa.lazy()
        .select('cle_rsa', 'zrdth', 'nb_rdth')
        .with_columns(
            [
                pl.col('zrdth').str.extract_all('.{7}').alias("RDTH")
            ]
            )
        .explode('RDTH')
        .select(["cle_rsa", "RDTH"])
        #.filter(~pl.col("RDTH").is_null())
        .with_columns(
            pl.col('RDTH').str.slice(0,4).alias('codsupra'),
            pl.col('RDTH').str.slice(5,2).cast(pl.Int32).alias('nbsupra')
            )
        .drop('RDTH')
        .with_columns(
            pl.when(pl.col('codsupra') == '9610').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9610'),
            pl.when(pl.col('codsupra') == '9619').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9619'),
            pl.when(pl.col('codsupra') == '9620').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9620'),
            pl.when(pl.col('codsupra') == '9621').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9621'),
            pl.when(pl.col('codsupra') == '9622').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9622'),
            pl.when(pl.col('codsupra') == '9625').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9625'),
            pl.when(pl.col('codsupra') == '9631').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9631'),
            pl.when(pl.col('codsupra') == '9632').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9632'),
            pl.when(pl.col('codsupra') == '9633').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9633'),
            pl.when(pl.col('codsupra') == '6523').then(pl.col('nbsupra')).otherwise(0).alias('nbacte6523'),
            pl.when(pl.col('codsupra') == '9623').then(pl.col('nbsupra')).otherwise(0).alias('nbacte9623')
            )
        .drop('codsupra', 'nbsupra')
        .group_by('cle_rsa')
        .sum()
        .collect()
        )

    rsa_valo = (rsa_valo
        .join(rdth, on = 'cle_rsa', how = 'left')
        )



    # préparer pie
    trans_pie = (
        pie
        .group_by('cle_rsa', 'code_pie')
        .agg(pl.col('nbsuppie').sum().alias('nbsuppie'))
        .join(pl.DataFrame({'liste_pie' : ['STF', 'SRC', 'REA', 'REP', 'NN1', 'NN2', 'NN3']}), left_on = 'code_pie', right_on = 'liste_pie', how = 'right')
        .with_columns(pl.col("nbsuppie").fill_null(strategy="zero"))
        .with_columns(pl.concat_str([pl.lit('pie_'), 'liste_pie']).str.to_lowercase().alias('liste_pie'))
        .pivot(index = 'cle_rsa', values = 'nbsuppie', on = 'liste_pie', aggregate_function = "sum")
        #.filter(~pl.col('cle_rsa').is_null())
        .join(rsa.select('cle_rsa'), how = 'right', left_on = 'cle_rsa', right_on = 'cle_rsa')
        .with_columns(pl.col("^pie.*$").fill_null(strategy="zero"))
    )

    # préparer dip
    dip = (
        diap
        .group_by('cle_rsa')
        .agg(pl.col('nbsup').sum().alias('nbdip'))
    )

    # préparer po
    po = (
        rsa_valo.select('cle_rsa').join(
            porg
            .with_columns(
                pl.when(pl.col('cdpo') == 'PO1').then(1).otherwise(0).alias('nb_poi'),
                pl.when(pl.col('cdpo') == 'PO2').then(1).otherwise(0).alias('nb_poii'),
                pl.when(pl.col('cdpo') == 'PO3').then(1).otherwise(0).alias('nb_poiii'),
                pl.when(pl.col('cdpo') == 'PO4').then(1).otherwise(0).alias('nb_poiv'),
                pl.when(pl.col('cdpo') == 'PO5').then(1).otherwise(0).alias('nb_pov'),
                pl.when(pl.col('cdpo') == 'PO6').then(1).otherwise(0).alias('nb_povi'),
                pl.when(pl.col('cdpo') == 'PO7').then(1).otherwise(0).alias('nb_povii'),
                pl.when(pl.col('cdpo') == 'PO8').then(1).otherwise(0).alias('nb_poviii'),
                pl.when(pl.col('cdpo') == 'PO9').then(1).otherwise(0).alias('nb_poix'),
                pl.when(pl.col('cdpo') == 'POA').then(1).otherwise(0).alias('nb_poa')
                )
            .group_by('cle_rsa')
            .agg(cs.starts_with('nb_').sum())
            .with_columns(
                pl.when(pl.col('nb_poiv') > 0).then(0).otherwise(pl.col('nb_poi')).alias('nb_poi'),
                pl.when(pl.col('nb_poiv') > 0).then(0).otherwise(pl.col('nb_poii')).alias('nb_poii'),
                pl.when(pl.col('nb_poiv') > 0).then(0).otherwise(pl.col('nb_poiii')).alias('nb_poiii')
                ), 
            how = "left", on = 'cle_rsa')
        .with_columns(cs.starts_with("nb_").fill_null(strategy="zero"))
    )

    if rsa_valo['anseqta'].unique().max() < '2017':
        rsa_valo = (
            rsa_valo
            .with_columns(pl.lit('0').alias('suppdefcard'))
            )

    if rsa_valo['anseqta'].unique().max() < '2023':
        rsa_valo = (
            rsa_valo
            .with_columns(pl.lit('0').alias('topctc'))
            )

    rsa_valo = (
        rsa_valo
        .join(dip, on = 'cle_rsa', how = 'left')
        .join(po, on = 'cle_rsa', how = 'left')
        .join(trans_pie, on = 'cle_rsa', how = 'left')
        .join(supplements, on = 'anseqta', how = 'left')
        .lazy()
        # suppléments structures
        .with_columns(
            (pl.col('trep') * pl.col('nbsuprep') * pl.col('cprudent') * cgeo).alias('rec_rep'),
            (pl.col('trea') * pl.col('nbsuprea') * pl.col('cprudent') * cgeo).alias('rec_rea'),
            (pl.col('tsi')  * pl.col('nbsupstf') * pl.col('cprudent') * cgeo).alias('rec_stf'),
            (pl.col('tsc')  * pl.col('nbsupsrc') * pl.col('cprudent') * cgeo).alias('rec_src'),
            (pl.col('tnn1') * pl.col('nbsupnn1') * pl.col('cprudent') * cgeo).alias('rec_nn1'),
            (pl.col('tnn2') * pl.col('nbsupnn2') * pl.col('cprudent') * cgeo).alias('rec_nn2'),
            (pl.col('tnn3') * pl.col('nbsupnn3') * pl.col('cprudent') * cgeo).alias('rec_nn3'),
        # suppléments dialyse hors séances
            (pl.col('thhs')     * pl.col('nbsuphs')   * pl.col('cprudent')    * cgeo).alias('rec_hhs'),
            (pl.col('tedpahs')  * pl.col('nbsupahs')  * pl.col('cprudent')    * cgeo).alias('rec_edpahs'),
            (pl.col('tedpcahs') * pl.col('nbsupehs')  * pl.col('cprudent')    * cgeo).alias('rec_edpcahs'),
            (pl.col('tehhs')    * pl.col('nbsupehs')  * pl.col('cprudent')    * cgeo).alias('rec_ehhs'),
            (pl.col('tdip')     * pl.col('nbdip')     * pl.col('cprudent')    * cgeo).alias('rec_dip'),
        # autres suppléments
            (pl.col('tcaishyp')  * pl.col('nbsupcaisson') * pl.col('cprudent') * cgeo).alias('rec_caishyp'),
            (pl.col('taph_9615') * pl.col('nbacte9615')   * pl.col('cprudent') * cgeo).alias('rec_aph'),
            (pl.col('tant')      * pl.col('nbsupatpart')  * pl.col('cprudent') * cgeo).alias('rec_ant'),
            (pl.col('trap')      * pl.col('nbsupreaped')  * pl.col('cprudent') * cgeo).alias('rec_rap'),
            (pl.col('sdc')       * pl.when(pl.col('suppdefcard') == '1').then(1).otherwise(0) * pl.col('cprudent') * cgeo).alias('rec_sdc'),
            (pl.col('ctc')       * pl.when(pl.col('topctc') == '1').then(1).otherwise(0)      * pl.col('cprudent') * cgeo).alias('rec_ctc'),
        # supplements irradiation hors séances
            (pl.col('trdt5_9610')   * pl.col('nbacte9610') * pl.col('cprudent') * cgeo).alias('rec_rdt5'),
            (pl.col('tprot_9619')   * pl.col('nbacte9619') * pl.col('cprudent') * cgeo).alias('rec_prot'),
            (pl.col('tict_9620')    * pl.col('nbacte9620') * pl.col('cprudent') * cgeo).alias('rec_ict'),
            (pl.col('tcyb_9621')    * pl.col('nbacte9621') * pl.col('cprudent') * cgeo).alias('rec_cyb'),
            (pl.col('tgam_6523')    * pl.col('nbacte6523') * pl.col('cprudent') * cgeo).alias('rec_gam'),
            (pl.col('trconf_9622')  * pl.col('nbacte9622') * pl.col('cprudent') * cgeo).alias('rec_rcon1'),
            (pl.col('trconf_9625')  * pl.col('nbacte9625') * pl.col('cprudent') * cgeo).alias('rec_rcon2'),
            (pl.col('ttciea_9631')  * pl.col('nbacte9631') * pl.col('cprudent') * cgeo).alias('rec_tciea'),
            (pl.col('ttcies_9632')  * pl.col('nbacte9632') * pl.col('cprudent') * cgeo).alias('rec_tcies'),
            (pl.col('taie_9633')    * pl.col('nbacte9633') * pl.col('cprudent') * cgeo).alias('rec_aie'),
            (pl.col('trconf_9623')  * pl.col('nbacte9623') * pl.col('cprudent') * cgeo).alias('rec_rcon3'),
       # po
           (pl.col('tpoi')   * pl.col('nb_poi') * pl.col('cprudent') * cgeo).alias('rec_poi'),
           (pl.col('tpoii')   * pl.col('nb_poii') * pl.col('cprudent') * cgeo).alias('rec_poii'),
           (pl.col('tpoiii')   * pl.col('nb_poiii') * pl.col('cprudent') * cgeo).alias('rec_poiii'),
           (pl.col('tpoiv')   * pl.col('nb_poiv') * pl.col('cprudent') * cgeo).alias('rec_poiv'),
           (pl.col('tpov')   * pl.col('nb_pov') * pl.col('cprudent') * cgeo).alias('rec_pov'),
           (pl.col('tpovi')   * pl.col('nb_povi') * pl.col('cprudent') * cgeo).alias('rec_povi'),
           (pl.col('tpovii')   * pl.col('nb_povii') * pl.col('cprudent') * cgeo).alias('rec_povii'),
           (pl.col('tpoviii')   * pl.col('nb_poviii') * pl.col('cprudent') * cgeo).alias('rec_poviii'),
           (pl.col('tpoix')   * pl.col('nb_poix') * pl.col('cprudent') * cgeo).alias('rec_poix'),
           (pl.col('tpoa')   * pl.col('nb_poa') * pl.col('cprudent') * cgeo).alias('rec_poa'),
        # pie
        (pl.col('tsc')       * pl.col('pie_src') * pl.col('cprudent') * cgeo).alias('rec_pie_src'),
        (pl.col('tsi')   * pl.col('pie_stf') * pl.col('cprudent') * cgeo).alias('rec_pie_stf'),
        (pl.col('trea')   * pl.col('pie_rea') * pl.col('cprudent') * cgeo).alias('rec_pie_rea'),
        (pl.col('trep')   * pl.col('pie_rep') * pl.col('cprudent') * cgeo).alias('rec_pie_rep'),
        (pl.col('tnn1')   * pl.col('pie_nn1') * pl.col('cprudent') * cgeo).alias('rec_pie_nn1'),
        (pl.col('tnn2')   * pl.col('pie_nn2') * pl.col('cprudent') * cgeo).alias('rec_pie_nn2'),
        (pl.col('tnn3')   * pl.col('pie_nn3') * pl.col('cprudent') * cgeo).alias('rec_pie_nn3'),
        )
        .with_columns(
            (pl.col('rec_src') + pl.col('rec_pie_src')).alias('rec_src'),
            (pl.col('rec_stf') + pl.col('rec_pie_stf')).alias('rec_stf'),
            (pl.col('rec_rea') + pl.col('rec_pie_rea')).alias('rec_rea'),
            (pl.col('rec_rep') + pl.col('rec_pie_rep')).alias('rec_rep'),
            (pl.col('rec_nn1') + pl.col('rec_pie_nn1')).alias('rec_nn1'),
            (pl.col('rec_nn2') + pl.col('rec_pie_nn2')).alias('rec_nn2'),
            (pl.col('rec_nn3') + pl.col('rec_pie_nn3')).alias('rec_nn3'),
        )
        .with_columns(
            pl.sum_horizontal('rec_hhs', 'rec_edpahs', 'rec_edpcahs', 'rec_ehhs', 'rec_dip').alias('rec_dialhosp'),
            pl.sum_horizontal('rec_rdt5', 'rec_prot', 'rec_ict', 'rec_cyb', 'rec_gam', 'rec_rcon1',
                'rec_rcon2', 'rec_tciea', 'rec_tcies', 'rec_aie', 'rec_rcon3').alias('rec_rdt_tot'),
            pl.sum_horizontal('rec_poi', 'rec_poii', 'rec_poiii', 'rec_poiv', 'rec_pov', 'rec_povi',
              'rec_povii', 'rec_poviii', 'rec_poix', 'rec_poa').alias('rec_po_tot')
            )
        .collect()
        )
        

    
    
    # calcul recette totale
    rsa_valo = (
        rsa_valo
        .with_columns(pl.lit(None, pl.Float64).alias('rec_rehosp_ghm'))
        .with_columns(
            pl.sum_horizontal(['rec_bee', 'rec_rep', 'rec_rea', 'rec_stf', 'rec_src', 'rec_nn1', 'rec_nn2', 'rec_nn3',
                'rec_dialhosp', 'rec_caishyp', 'rec_aph', 'rec_ant', 'rec_rap', 'rec_rehosp_ghm', 'rec_rdt_tot', 'rec_po_tot', 'rec_sdc', 'rec_ctc'])
            .alias('rec_totale')
            )
        )

    if full:
        return (
            rsa_valo
            .rename({'t_bas' : 'rec_exb', 't_base' : 'rec_base', 't_haut' : 'rec_exh'})
            )

    return (
            rsa_valo
            .rename({'t_bas' : 'rec_exb', 't_base' : 'rec_base', 't_haut' : 'rec_exh'})
            .select('cle_rsa', 'nbseance', 'moissor', 'anseqta', 'rec_totale', 'rec_bee', 'rec_base', 'rec_exb', 'rec_exh',
                                     'rec_rep', 'rec_rea', 'rec_stf', 'rec_src', 'rec_nn1', 'rec_nn2', 'rec_nn3',
                                     'rec_dialhosp', 'rec_caishyp', 'rec_aph', 'rec_ant', 'rec_rap', 'rec_rehosp_ghm',
                                     'rec_rdt_tot', 'rec_sdc', 'rec_ctc', 'rec_po_tot')
            # ajouter les *effectifs*
            )

























