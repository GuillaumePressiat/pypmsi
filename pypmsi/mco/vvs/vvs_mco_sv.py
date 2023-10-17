
import polars as pl
import polars.selectors as cs
from pypmsi.utils import *

def vvs_mco_sv(rsa, ano, porg = pl.DataFrame({'cle_rsa' : ''})):

    # CM 90
    # Séjours en CM 90 : nombre de RSA groupés dans la CM 90 (groupage en erreur)
    un = (
        rsa
        .filter(pl.col('rsacmd') == '90')
        .unique('cle_rsa')
        .select('cle_rsa')
        )

    # PIE
    # Séjours de prestation inter-établissement (PIE)
    # : nombre de RSA avec type de séjour=’B’ (hors séances de RDTH, dialyse et chimiothérapie, car les PIE sont valorisés pour ces RSA). . Sont 
    # ainsi dénombrés les séjours effectués dans l’établissement de santé prestataire (type de séjour = ‘B’) à la demande d’un établissement (type de séjour = ‘A’) pour la réalisation d’un acte
    # médicotechnique ou d’une autre prestation.
    deux = (
        rsa
        .filter(
            (pl.col('typesej') == 'B') &
            ~pl.col('ghm').is_in(['28Z03Z','28Z04Z','28Z07Z','28Z08Z','28Z09Z','28Z10Z','28Z11Z','28Z12Z','28Z13Z','28Z17Z','28Z18Z',
                                                             '28Z19Z','28Z20Z','28Z21Z','28Z22Z','28Z23Z','28Z23Z','28Z24Z','28Z25Z'])
            )
        .unique('cle_rsa')
        .select('cle_rsa')
        )

    # RSA avec GHS 9999 : nombre de RSA avec GHS = 9999, en dehors de ceux en CM 90
    trois = (
        rsa
        .filter(pl.col('noghs') == '9999')
        .join(un, on = 'cle_rsa', how = 'anti')
        )


    # Table intermédiaire 1
    autres1 = (
        rsa
        .filter(
            (~pl.col('ghm').str.slice(0,5).is_in(['28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25'])) |
            ~((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))
        )
        .unique('cle_rsa')
        .select('cle_rsa')
        .join(un, on = 'cle_rsa', how = 'anti')
        .join(deux, on = 'cle_rsa', how = 'anti')
        .join(porg, on = 'cle_rsa', how = 'anti')
        .join(trois, on = 'cle_rsa', how = 'anti')
        )


    # Table intermédiaire 2
    autres2 = (
        rsa
        .unique('cle_rsa')
        .select('cle_rsa')
        .join(un, on = 'cle_rsa', how = 'anti')
        .join(deux, on = 'cle_rsa', how = 'anti')
        .join(porg, on = 'cle_rsa', how = 'anti')
        .join(trois, on = 'cle_rsa', how = 'anti')
        )


    # Forfait journalier non applicable
    fjnap = (
        rsa
        .filter(
            (pl.col('rsacmd') == '28')  |
            (pl.col('duree') == 0)      |
            (pl.col('ghm') == '14Z08Z') |
            (pl.col('ghm') == '23K02Z') |
            ((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))
        )
        .unique('cle_rsa')
        .select('cle_rsa')
        .with_columns(pl.lit(1).alias('fjnap'))
        )

    # rdth nnés
    autres3 = (
        rsa
        .filter(
            (pl.col('ghm').str.slice(0,5).is_in(['28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25'])) |
            ((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))
            )
        )

    # séjours avec pb de chaînage (hors NN, rdth et PO)
    if 'cdgestion' in ano.columns:
        quatre = (
            ano
            .filter(
                (
                    (pl.col('crfushosp') != '0') | 
                    (pl.col('crfuspmsi') != '0') 
                ) &  (pl.col('cdgestion') != '65')
            )
            .unique('cle_rsa')
            .select('cle_rsa')
            .join(autres1, on = 'cle_rsa', how = 'inner')
            .join(autres3, on = 'cle_rsa', how = 'anti')
            )
    else:
        quatre = (
            ano
            .filter(
                (
                    (pl.col('crfushosp') != '0') | 
                    (pl.col('crfuspmsi') != '0')
                )
            )
            .unique('cle_rsa')
            .select('cle_rsa')
            .join(autres1, on = 'cle_rsa', how = 'inner')
            .join(autres3, on = 'cle_rsa', how = 'anti')
            )

    # attente de décision sur les droits du patient (hors NN, rdth et PO)
    six = (
        ano
        .filter(pl.col('factam') == '3')
        .unique('cle_rsa')
        .select('cle_rsa')
        .join(autres1, on = 'cle_rsa', how = 'inner')
        .join(autres3, on = 'cle_rsa', how = 'anti')
        )

    # séjours non facturables à l AM (sejours sans PO)
    sept = (
        ano
        .filter(pl.col('factam') == '0')
        .unique('cle_rsa')
        .select('cle_rsa')
        .join(un   , on = 'cle_rsa', how = 'anti')
        .join(deux , on = 'cle_rsa', how = 'anti')
        .join(trois, on = 'cle_rsa', how = 'anti')
        .join(porg , on = 'cle_rsa', how = 'anti')
        .join(six  , on = 'cle_rsa', how = 'anti')
        )

    # séjours avec PO sur patient arrivé décédé ou avec PO non facturables à l AM
    huit = (
        ano
        .filter(pl.col('factam') == '0')
        .unique('cle_rsa')
        .select('cle_rsa')
        .join(un   , on = 'cle_rsa', how = 'anti')
        .join(deux , on = 'cle_rsa', how = 'anti')
        .join(trois, on = 'cle_rsa', how = 'anti')
        .join(six  , on = 'cle_rsa', how = 'anti')
        .join(porg , on = 'cle_rsa', how = 'inner')
        )

    # caractère bloquant
    # https://www.atih.sante.fr/suppression-du-taux-de-conversion

    items_pour_bloquants = (
        ano
        .join(fjnap, on = 'cle_rsa', how  = 'left')
        .join(rsa.select('cle_rsa', 'agean', 'agejr', 'ghm', 'rsacmd', 'duree'), on = 'cle_rsa', how = 'inner')
        )

    caractere_bloquant = pl.concat(
        [
        (
            items_pour_bloquants
            .filter(pl.col('factam') == '0')
            .with_columns(
                pl.lit(1).alias('typvidhosp'),
                (pl.col('cdexticm') == 'X').alias('bextic'),
                (pl.col('cdprfojo') == 'X').alias('bfojo'),
                (pl.col('natass') == 'XX').alias('bnatass'),
                (~pl.col('nbeven').is_null()).alias('beven')

            )
        ),
        (
            items_pour_bloquants
            .filter(pl.col('factam') == '3')
            .with_columns(
                pl.lit(2).alias('typvidhosp'),
                (pl.col('cdexticm') == 'X').alias('bextic'),
                (pl.col('cdprfojo') == 'X').alias('bfojo'),
                (pl.col('natass') == 'XX').alias('bnatass'),
                (~pl.col('nbeven').is_null()).alias('beven')

            )
        ),
        (
            items_pour_bloquants
            .filter(pl.col('factam') == '2')
            .with_columns(
                pl.lit(3).alias('typvidhosp'),
                (pl.col('cdexticm').is_in(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C', 'X'])).alias('bextic'),
                (pl.col('cdprfojo').is_in(['A', 'L', 'R', 'X'])).alias('bfojo'),
                (pl.col('natass').is_in(['10', '13', '30', '41', '90', 'XX'])).alias('bnatass'),
                (~pl.col('nbeven').is_null()).alias('beven')

            )
        ),
        (# nv-né
            items_pour_bloquants
            .filter(pl.col('factam') == '1')
            .filter(((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null())))
            .with_columns(
                pl.lit(4).alias('typvidhosp'),
                (pl.col('cdexticm') == 'X').alias('bextic'),
                (pl.col('cdprfojo') == 'X').alias('bfojo'),
                (pl.col('natass') == 'XX').alias('bnatass'),
                (pl.col('nbeven')== 1).alias('beven')
            )
        ),
        (# radiotherapie
            items_pour_bloquants
            .filter(pl.col('factam') == '1')
            .filter(
                (pl.col('ghm').str.slice(0,5).is_in(['28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25'])) &
                ~((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))
                )
            .with_columns(
                pl.lit(5).alias('typvidhosp'),
                (pl.col('cdexticm').is_in(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C', 'X'])).alias('bextic'),
                (pl.col('cdprfojo') == 'X').alias('bfojo'),
                (pl.col('natass').is_in(['10', '13', '30', '41', '90', 'XX'])).alias('bnatass'),
                (~pl.col('nbeven').is_null()).alias('beven')
            )
        ),
        (# séances hors radiotherapie
            items_pour_bloquants
            .filter(pl.col('factam') == '1')
            .filter(
                (pl.col('rsacmd') == '28') & 
                ~(pl.col('ghm').str.slice(0,5).is_in(['28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25'])) &
                ~((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))
                )
            .with_columns(
                pl.lit(6).alias('typvidhosp'),
                (pl.col('cdexticm').is_in(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C'])).alias('bextic'),
                (pl.col('cdprfojo') == 'X').alias('bfojo'),
                (pl.col('natass').is_in(['10', '13', '30', '41', '90'])).alias('bnatass'),
                (~pl.col('nbeven').is_null()).alias('beven')
            )
        ),
        (# durée = 0 j
            items_pour_bloquants
            .filter(pl.col('factam') == '1')
            .filter(
                (pl.col('duree') == 0) & 
                ~(pl.col('rsacmd') == '28') & 
                ~(pl.col('ghm').str.slice(0,5).is_in(['28Z11', '28Z18', '28Z19', '28Z20', '28Z21', '28Z22', '28Z23', '28Z24', '28Z25'])) &
                ~((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))
                )
            .with_columns(
                pl.lit(7).alias('typvidhosp'),
                (pl.col('cdexticm').is_in(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C'])).alias('bextic'),
                (pl.col('cdprfojo') == 'X').alias('bfojo'),
                (pl.col('natass').is_in(['10', '13', '30', '41', '90'])).alias('bnatass'),
                (~pl.col('nbeven').is_null()).alias('beven')
            )
        ),
        (# le complémentaire
            items_pour_bloquants
            .filter(pl.col('factam') == '1')
            .filter(
                ~(pl.col('duree') == 0) & 
                ~(pl.col('rsacmd') == '28') & 
                ~((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))
                )
            .with_columns(
                pl.lit(8).alias('typvidhosp'),
                (pl.col('cdexticm').is_in(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'C'])).alias('bextic'),
                (pl.col('cdprfojo').is_in(['A', 'L', 'R'])).alias('bfojo'),
                (pl.col('natass').is_in(['10', '13', '30', '41', '90'])).alias('bnatass'),
                (pl.col('nbeven') == 1).alias('beven')
            )
        ),
        ]).select('cle_rsa', 'typvidhosp', 'bextic', 'bnatass', 'beven', 'bfojo')

    
    items_pour_cinq = (
        items_pour_bloquants
        .join(caractere_bloquant, on = 'cle_rsa', how = 'inner')
        )

    bloq = pl.concat([
        (items_pour_cinq
            .filter(pl.col('tauxrm').is_in(['08000', '09000', '10000']))
            .filter((pl.col('fjnap').is_null()) & (pl.col('bfojo').not_()))
        ),
        (items_pour_cinq
            .filter(~pl.col('tauxrm').is_in(['08000', '09000', '10000']))
            .filter(
                ((pl.col('fjnap').is_null()) & (pl.col('bfojo').not_())) |
                (pl.col('bnatass').not_()) |
                (pl.col('bextic').not_())
                )
        )]) # pl.col('beven').not_()

    ok = pl.concat([
        un.select('cle_rsa').with_columns(pl.lit(1).alias('type')),
        deux.select('cle_rsa').with_columns(pl.lit(2).alias('type')),
        trois.select('cle_rsa').with_columns(pl.lit(3).alias('type')),
        quatre.select('cle_rsa').with_columns(pl.lit(4).alias('type')),
        six.select('cle_rsa').with_columns(pl.lit(6).alias('type')),
        sept.select('cle_rsa').with_columns(pl.lit(7).alias('type')),
        huit.select('cle_rsa').with_columns(pl.lit(8).alias('type'))
        ])

    if 'cdgestion' in ano.columns:
        bloq1 = (
            bloq
            .join(ok, on = 'cle_rsa', how = 'anti')
            .join(porg,on = 'cle_rsa', how = 'anti')
            .join(rsa.filter(((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))), on = 'cle_rsa', how = 'anti')
            .join(ano.filter(pl.col('cdgestion') == '65'), on = 'cle_rsa', how = 'anti')
            )
    else:
        bloq1 = (
            bloq
            .join(ok, on = 'cle_rsa', how = 'anti')
            .join(porg,on = 'cle_rsa', how = 'anti')
            .join(rsa.filter(((pl.col('agejr') <= 30) & (pl.col('agejr') >= 0) & ((pl.col('agean') == 0) | pl.col('agean').is_null()))), on = 'cle_rsa', how = 'anti')
            #.join(ano.filter(pl.col('cdgestion') == '65'), on = 'cle_rsa', how = 'anti')
            )

    df_final = (rsa
        .select('cle_rsa')
        .unique('cle_rsa')
        .join(ok.select('cle_rsa', 'type'), on = 'cle_rsa', how = 'left')
        .join(bloq1.select('cle_rsa').with_columns(pl.lit(1).alias('bloq')), on = 'cle_rsa', how = 'left')
        .join(caractere_bloquant.select('cle_rsa', 'typvidhosp'), on = 'cle_rsa', how = 'left')
        .with_columns(
            pl.when(
                (pl.col('type').is_null() & pl.col('bloq').is_not_null())).then(5)
            .when((pl.col('type').is_not_null() & pl.col('bloq').is_not_null() )).then(pl.col('type'))
            .when((pl.col('type').is_not_null() & pl.col('bloq').is_null() )).then(pl.col('type'))
            .otherwise(0).alias('type_fin')
            )
        .select('cle_rsa', 'type', 'type_fin', 'typvidhosp')
        )


    return df_final

