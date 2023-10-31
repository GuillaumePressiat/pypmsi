
import polars as pl

vvs_mco_lib_detail_valo = pl.DataFrame([
    {
        "var": "nb_poa",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_poi",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_poii",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_poiii",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_poiv",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_poix",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_pov",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_povi",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_povii",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nb_poviii",
        "libelle_detail_valo": "Supplément prélèvement d'organe"
    },
    {
        "var": "nbacte6523",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9610",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9615",
        "libelle_detail_valo": "Supplément GHS 9615"
    },
    {
        "var": "nbacte9619",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9620",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9621",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9622",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9623",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9625",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9631",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9632",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbacte9633",
        "libelle_detail_valo": "Supplément RDTH en hopsit"
    },
    {
        "var": "nbdip",
        "libelle_detail_valo": "Supplément dialyses"
    },
    {
        "var": "nbjrbs",
        "libelle_detail_valo": "Extrêmes haut"
    },
    {
        "var": "nbjrexb",
        "libelle_detail_valo": "Extrêmes bas"
    },
    {
        "var": "nbrum",
        "libelle_detail_valo": "Nb RUM"
    },
    {
        "var": "nbseance",
        "libelle_detail_valo": "Nb séjours / séances"
    },
    {
        "var": "nbsupahs",
        "libelle_detail_valo": "Supplément dialyses"
    },
    {
        "var": "nbsupatpart",
        "libelle_detail_valo": "Supplément Ante partum"
    },
    {
        "var": "nbsupcaisson",
        "libelle_detail_valo": "Supplément caisson hyperbare"
    },
    {
        "var": "nbsupchs",
        "libelle_detail_valo": "Supplément dialyses"
    },
    {
        "var": "nbsupehs",
        "libelle_detail_valo": "Supplément dialyses"
    },
    {
        "var": "nbsuphs",
        "libelle_detail_valo": "Supplément dialyses"
    },
    {
        "var": "nbsupnn1",
        "libelle_detail_valo": "Supplément néonat sans SI"
    },
    {
        "var": "nbsupnn2",
        "libelle_detail_valo": "Supplément néonat avec SI"
    },
    {
        "var": "nbsupnn3",
        "libelle_detail_valo": "Supplément réanimation néonat"
    },
    {
        "var": "nbsuprea",
        "libelle_detail_valo": "Supplément réanimation"
    },
    {
        "var": "nbsupreaped",
        "libelle_detail_valo": "Supplément radiothérapie pédiatrique"
    },
    {
        "var": "nbsuprep",
        "libelle_detail_valo": "Supplément réanimation pédiatrique"
    },
    {
        "var": "nbsupsrc",
        "libelle_detail_valo": "Supplément surveillance continue"
    },
    {
        "var": "nbsupstf",
        "libelle_detail_valo": "Supplément soins intensifs"
    },
    {
        "var": "pie_nn1",
        "libelle_detail_valo": "Supplément néonat sans SI"
    },
    {
        "var": "pie_nn2",
        "libelle_detail_valo": "Supplément néonat avec SI"
    },
    {
        "var": "pie_nn3",
        "libelle_detail_valo": "Supplément réanimation néonat"
    },
    {
        "var": "pie_rea",
        "libelle_detail_valo": "Supplément réanimation"
    },
    {
        "var": "pie_rep",
        "libelle_detail_valo": "Supplément réanimation pédiatrique"
    },
    {
        "var": "pie_src",
        "libelle_detail_valo": "Supplément surveillance continue"
    },
    {
        "var": "pie_stf",
        "libelle_detail_valo": "Supplément soins intensifs"
    },
    {
        "var": "rehosp_ghm",
        "libelle_detail_valo": "Réhospitalisation dans le même GHM"
    }
], schema={"var" : pl.Utf8, "libelle_detail_valo" : pl.Utf8})


vvs_mco_lib_valo = pl.DataFrame([
    {
        "ordre_epmsi": 22,
        "rubrique": "rec_totale",
        "lib_valo": "Valorisation 100% T2A globale"
    },
    {
        "ordre_epmsi": 1,
        "rubrique": "rec_base",
        "lib_valo": "Valorisation des GHS de base"
    },
    {
        "ordre_epmsi": 0,
        "rubrique": "rec_bee",
        "lib_valo": "Valorisation base + exb + exh"
    },
    {
        "ordre_epmsi": 2,
        "rubrique": "rec_exb",
        "lib_valo": "Valorisation extrême bas (à déduire)"
    },
    {
        "ordre_epmsi": 3,
        "rubrique": "rec_rehosp_ghm",
        "lib_valo": "Valorisation séjours avec rehosp dans même GHM"
    },
    {
        "ordre_epmsi": 4,
        "rubrique": "rec_mino_sus",
        "lib_valo": "Valorisation séjours avec minoration forfaitaire liste en sus"
    },
    {
        "ordre_epmsi": 5,
        "rubrique": "rec_exh",
        "lib_valo": "Valorisation journées extrême haut"
    },
    {
        "ordre_epmsi": 6,
        "rubrique": "rec_aph",
        "lib_valo": "Valorisation actes GHS 9615 en Hospit."
    },
    {
        "ordre_epmsi": 7,
        "rubrique": "rec_rap",
        "lib_valo": "Valorisation suppléments radiothérapie pédiatrique"
    },
    {
        "ordre_epmsi": 8,
        "rubrique": "rec_ant",
        "lib_valo": "Valorisation suppléments antepartum"
    },
    {
        "ordre_epmsi": 9,
        "rubrique": "rec_rdt_tot",
        "lib_valo": "Valorisation actes RDTH en Hospit."
    },
    {
        "ordre_epmsi": 10,
        "rubrique": "rec_rea",
        "lib_valo": "Valorisation suppléments de réanimation"
    },
    {
        "ordre_epmsi": 11,
        "rubrique": "rec_rep",
        "lib_valo": "Valorisation suppléments de réa pédiatrique"
    },
    {
        "ordre_epmsi": 12,
        "rubrique": "rec_nn1",
        "lib_valo": "Valorisation suppléments de néonat sans SI"
    },
    {
        "ordre_epmsi": 13,
        "rubrique": "rec_nn2",
        "lib_valo": "Valorisation suppléments de néonat avec SI"
    },
    {
        "ordre_epmsi": 14,
        "rubrique": "rec_nn3",
        "lib_valo": "Valorisation suppléments de réanimation néonat"
    },
    {
        "ordre_epmsi": 15,
        "rubrique": "rec_po_tot",
        "lib_valo": "Valorisation prélévements d organe"
    },
    {
        "ordre_epmsi": 16,
        "rubrique": "rec_caishyp",
        "lib_valo": "Valorisation des actes de caissons hyperbares en sus"
    },
    {
        "ordre_epmsi": 17,
        "rubrique": "rec_dialhosp",
        "lib_valo": "Valorisation suppléments de dialyse"
    },
    {
        "ordre_epmsi": 18,
        "rubrique": "rec_sdc",
        "lib_valo": "Valorisation supplément défibrilateur cardiaque"
    },
    {
        "ordre_epmsi": 19,
        "rubrique": "rec_i04",
        "lib_valo": "Valorisation suppléments Forfait Innovation I04"
    },
    {
        "ordre_epmsi": 20,
        "rubrique": "rec_ctc",
        "lib_valo": "Valorisation suppléments Car-t Cells"
    },
    {
        "ordre_epmsi": 21,
        "rubrique": "rec_src",
        "lib_valo": "Valorisation suppléments de surveillance continue"
    },
    {
        "ordre_epmsi": 22,
        "rubrique": "rec_stf",
        "lib_valo": "Valorisation suppléments de soins intensifs"
    }
], schema={"ordre_epmsi": pl.Int32, "rubrique" : pl.Utf8, "lib_valo" : pl.Utf8})


vvs_mco_lib_type_sej = pl.DataFrame([
    {
        "type_fin": 0,
        "lib_type": "Séjours valorisés"
    },
    {
        "type_fin": 1,
        "lib_type": "Séjours en CM 90"
    },
    {
        "type_fin": 2,
        "lib_type": "Séjours en prestation inter-établissement"
    },
    {
        "type_fin": 3,
        "lib_type": "Séjours en GHS 9999"
    },
    {
        "type_fin": 4,
        "lib_type": "Séjours avec pb de chainage (hors NN, rdth et PO)"
    },
    {
        "type_fin": 5,
        "lib_type": "Séjours avec pb de codage des variables bloquantes"
    },
    {
        "type_fin": 6,
        "lib_type": "Séjours en attente de décision sur les droits du patient"
    },
    {
        "type_fin": 7,
        "lib_type": "Séjours non facturable à l'AM hors PO"
    },
    {
        "type_fin": 8,
        "lib_type": "Séjours avec PO sur patient arrivé décédé ou avec PO non facturables à l'AM"
    }
], schema={"type_fin": pl.Int32, "lib_type" : pl.Utf8})


vvs_mco_lib_typvidhosp = pl.DataFrame([
    {
        "typvidhosp": 1,
        "lib_typvidhosp": "0 : non pris en charge"
    },
    {
        "typvidhosp": 2,
        "lib_typvidhosp": "3 : En attente des droits du patient"
    },
    {
        "typvidhosp": 3,
        "lib_typvidhosp": "2 : En attente sur le taux de prise en charge"
    },
    {
        "typvidhosp": 4,
        "lib_typvidhosp": "1 : Nouveau-né"
    },
    {
        "typvidhosp": 5,
        "lib_typvidhosp": "1 : Radiothérapie"
    },
    {
        "typvidhosp": 6,
        "lib_typvidhosp": "1 : Séances hors radiothérapie"
    },
    {
        "typvidhosp": 7,
        "lib_typvidhosp": "1 : Durée de séjour = 0"
    },
    {
        "typvidhosp": 8,
        "lib_typvidhosp": "1 : Autre type de séjour"
    }
], schema={"typvidhosp": pl.Int32, "lib_typvidhosp" : pl.Utf8})


def vvs_mco_libelles_valo():
    return {
    'vvs_mco_lib_detail_valo' : vvs_mco_lib_detail_valo,
    'vvs_mco_lib_valo' : vvs_mco_lib_valo,
    'vvs_mco_lib_type_sej' : vvs_mco_lib_type_sej,
    'vvs_mco_lib_typvidhosp' : vvs_mco_lib_typvidhosp
    }
