from flask import jsonify, request, Response
from app import app
import os
import pandas as pd
from datetime import datetime
from .Functions.Utils import (
    get_files_in_directory,
    should_aggregate_monthly,
    aggregate_time_series,
    Metrics_DATA_Filters,
)
from .CHARTS.Volumedata import prepare_volume_data
from .CHARTS.VolumeByProducts import prepare_volume_data_by_product
from .CHARTS.CANetByProducts import prepare_ca_net_by_product
from .CHARTS.VoyagesRendus import prepare_voyages_rendus_data
from .CHARTS.CANetandCABrut import prepare_ca_data
from .CHARTS.PerformanceCommercialAndFinancier import (
    prepare_performance_créance_commerciale_recouvrement,
)
from .CHARTS.PMVGlobal import prepare_pmv_data
from .CHARTS.TopSixClients import prepare_top_six_clients
from flask_cors import CORS, cross_origin


def Metrics(filtered_data, group_by_month, args, df_recouvrement, debut_date, fin_date):
    RECOUVREMENT_DATA = df_recouvrement
    RECOUVREMENT_DATA["Date de Paiement"] = pd.to_datetime(
        RECOUVREMENT_DATA["Date de Paiement"], format="%d/%m/%Y", errors="coerce"
    )
    RECOUVREMENT_DATA = RECOUVREMENT_DATA[
        (RECOUVREMENT_DATA["Date de Paiement"].dt.date >= debut_date.date())
        & (RECOUVREMENT_DATA["Date de Paiement"].dt.date <= fin_date.date())
    ]

    RECOUVREMENT_DATA["Date de Paiement"] = RECOUVREMENT_DATA[
        "Date de Paiement"
    ].dt.strftime("%d/%m/%Y")
    nobles_filtered = filtered_data[filtered_data["Type"] == "Nobles"].copy()
    graves_filtered = filtered_data[filtered_data["Type"] == "Graves"].copy()
    steriles_filtered = filtered_data[filtered_data["Type"] == "Stérile"].copy()
    En_espece_filtered = filtered_data[filtered_data["BC"] == "EN ESPECE"].copy()

    CA_NET_NOBLES = Metrics_DATA_Filters(nobles_filtered, group_by_month)[
        "CA Net"
    ].sum()
    CA_NET_GRAVES = Metrics_DATA_Filters(graves_filtered, group_by_month)[
        "CA Net"
    ].sum()
    CA_NET_STERILES = Metrics_DATA_Filters(steriles_filtered, group_by_month)[
        "CA Net"
    ].sum()
    QNT_NET_NOBLES = Metrics_DATA_Filters(nobles_filtered, group_by_month)[
        "Qté en T"
    ].sum()
    QNT_NET_GRAVES = Metrics_DATA_Filters(graves_filtered, group_by_month)[
        "Qté en T"
    ].sum()
    QNT_NET_STERILES = Metrics_DATA_Filters(steriles_filtered, group_by_month)[
        "Qté en T"
    ].sum()

    # Calculate totals used in both conditions
    DATA = aggregate_time_series(
        filtered_data,
        "Date",
        [
            "CA BRUT",
            "CA Net",
            "Qté en T",
            "Qté en m3",
            "Type",
            "CA Transport",
            "Coût de transport",
            "Marge sur Transport",
        ],
        group_by_month,
    )
    global CA_BRUT_TOTAL, CA_NET_TOTAL, QNT_EN_TONNE_TOTAL, MARGE_TRANSPORT, CA_TRANSPORT, COUT_TRANSPORT
    CA_BRUT_TOTAL = DATA["CA BRUT"].sum()
    CA_NET_TOTAL = DATA["CA Net"].sum()
    QNT_EN_TONNE_TOTAL = DATA["Qté en T"].sum()
    MARGE_TRANSPORT = DATA["Marge sur Transport"].sum()
    CA_TRANSPORT = DATA["CA Transport"].sum()
    COUT_TRANSPORT = DATA["Coût de transport"].sum()

    if args == "METRICS#1":
        global PMV_GLOBAL
        PMV_GLOBAL = CA_NET_TOTAL / QNT_EN_TONNE_TOTAL if QNT_EN_TONNE_TOTAL != 0 else 0

        return {
            "METRICS_CA_BRUT": CA_BRUT_TOTAL,
            "METRICS_CA_NET": CA_NET_TOTAL,
            "METRICS_PMV_GLOBAL": PMV_GLOBAL,
            "METRICS_QNT_EN_TONNE_GLOBALE": QNT_EN_TONNE_TOTAL,
            "METRICS_PMV_HORS_STERILE": (
                (CA_NET_NOBLES + CA_NET_GRAVES) / (QNT_NET_NOBLES + QNT_NET_GRAVES)
                if (QNT_NET_NOBLES + QNT_NET_GRAVES) != 0
                else 0
            ),
            "METRICS_MARGE_TRANSPORT": MARGE_TRANSPORT,
        }
    elif args == "METRICS#2":
        MIX_PRODUCT = (
            QNT_NET_NOBLES / QNT_EN_TONNE_TOTAL if QNT_EN_TONNE_TOTAL != 0 else 0
        )

        CAISSE_ESPECE = Metrics_DATA_Filters(En_espece_filtered, group_by_month)[
            "CA BRUT"
        ].sum()
        VOYAGES_RENDUS = prepare_voyages_rendus_data(filtered_data, group_by_month)
        global RECOUVREMENT
        RECOUVREMENT = RECOUVREMENT_DATA["Montant Paye"].tolist()
        return {
            "MIX_PRODUCT": MIX_PRODUCT,
            "CAISSE_ESPECE": CAISSE_ESPECE,
            "VOYAGES_RENDUS": sum(VOYAGES_RENDUS["GRAPHVOYAGERENDULIVREE"]),
            "RECOUVREMENT_EFFECTUER": sum(RECOUVREMENT),
        }


@app.route("/API/V1/BalanceSheet", methods=["POST"])
@cross_origin()
def balance_sheet():
    try:
        # Get and validate input dates
        debut_date = request.json.get("DébutDate")
        fin_date = request.json.get("FinDate")

        if not debut_date or not fin_date:
            return jsonify({"Message": "DébutDate and FinDate are required."}), 400

        try:
            debut_date = pd.to_datetime(debut_date, format="%d/%m/%Y")
            fin_date = pd.to_datetime(fin_date, format="%d/%m/%Y")
        except ValueError:
            return jsonify({"Message": "Invalid date format. Use DD/MM/YYYY."}), 400

        if fin_date < debut_date:
            return (
                jsonify({"Message": "FinDate cannot be earlier than DébutDate."}),
                400,
            )

        # Determine aggregation type
        group_by_month = should_aggregate_monthly(debut_date, fin_date)

        # Specific file path handling
        year = str(debut_date.year)
        source_path = os.path.join(".", "app/Source", year)

        expected_file = f"Source {year}.xlsx"
        target_file = os.path.join(source_path, expected_file)

        # Check if directory exists
        if not os.path.exists(source_path):
            return (
                jsonify(
                    {
                        "Message": f"Year directory not found: {source_path}",
                        "Available_Years": [
                            d
                            for d in os.listdir("./app/Source")
                            if os.path.isdir(os.path.join("./app/Source", d))
                        ],
                    }
                ),
                404,
            )

        # Check if file exists
        if not os.path.exists(target_file):
            return (
                jsonify(
                    {
                        "Message": f"Excel file not found: {expected_file}",
                        "Available_Files": os.listdir(source_path),
                    }
                ),
                404,
            )

        try:
            ventes_df = pd.read_excel(target_file, sheet_name="VENTES")
            recouvrement_df = pd.read_excel(target_file, sheet_name="RECOUVREMENT")
            commercials_objectifs_df = pd.read_excel(
                target_file, sheet_name="OBJECTIFS"
            )
        except Exception as e:
            return (
                jsonify(
                    {
                        "Message": f"Error reading Excel file: {str(e)}",
                        "File": target_file,
                    }
                ),
                500,
            )

        # Parse dates and filter data
        ventes_df["Date"] = pd.to_datetime(
            ventes_df["Date"], format="%d/%m/%Y", errors="coerce"
        )
        recouvrement_df["Date de Paiement"] = pd.to_datetime(
            recouvrement_df["Date de Paiement"], format="%d/%m/%Y", errors="coerce"
        )

        ventes_df.dropna(subset=["Date"], inplace=True)
        recouvrement_df.dropna(subset=["Date de Paiement"], inplace=True)

        # Filter data based on date range
        filtered_ventes = ventes_df[
            (ventes_df["Date"].dt.date >= debut_date.date())
            & (ventes_df["Date"].dt.date <= fin_date.date())
        ]

        filtered_recouvrement = recouvrement_df[
            (recouvrement_df["Date de Paiement"].dt.date >= debut_date.date())
            & (recouvrement_df["Date de Paiement"].dt.date <= fin_date.date())
        ]

        # Handle empty data case
        if filtered_ventes.empty and filtered_recouvrement.empty:
            available_dates = {
                "VENTES": ventes_df["Date"]
                .dt.strftime("%d/%m/%Y")
                .drop_duplicates()
                .tolist(),
                "RECOUVREMENT": recouvrement_df["Date de Paiement"]
                .dt.strftime("%d/%m/%Y")
                .drop_duplicates()
                .tolist(),
            }
            return (
                jsonify(
                    {
                        "Message": "No data found between the specified dates.",
                        "Available_Dates": available_dates,
                    }
                ),
                404,
            )

        # Prepare chart data

        chart_data = {
            "VOLGRAPH": prepare_volume_data(filtered_ventes, group_by_month),
            "CAGRAPH": prepare_ca_data(filtered_ventes, group_by_month),
            "PMVGRAPH": prepare_pmv_data(filtered_ventes, group_by_month),
            "COMMANDEGRAPH": prepare_voyages_rendus_data(
                filtered_ventes, group_by_month
            ),
            "QNTBYPRODUITGRAPH": prepare_volume_data_by_product(
                filtered_ventes, group_by_month
            ),
            "CANETBYPRODUITGRAPH": prepare_ca_net_by_product(
                filtered_ventes, group_by_month
            ),
            "TOP6CLIENTSGRAPH": prepare_top_six_clients(
                filtered_ventes, group_by_month
            ),
            "PERFORMANCECREANCEGRAPH": prepare_performance_créance_commerciale_recouvrement(
                target_file, debut_date, fin_date
            ),
        }

        # Prepare metrics data
        metrics_data = {
            "METRICS_ONE": Metrics(
                filtered_ventes,
                group_by_month,
                "METRICS#1",
                filtered_recouvrement,
                debut_date,
                fin_date,
            ),
            "METRICS_TWO": Metrics(
                filtered_ventes,
                group_by_month,
                "METRICS#2",
                filtered_recouvrement,
                debut_date,
                fin_date,
            ),
        }
        Table_Objectifs_DATA = {
            "CA_BRUT_OBJECTIF": commercials_objectifs_df.to_dict()["CA BRUT OBJ"][0],
            "CA_BRUT": CA_BRUT_TOTAL,
            #############################################################################
            "CA_NET_OBJECTIF": commercials_objectifs_df.to_dict()["CA NET OBJ"][0],
            "CA_NET": CA_NET_TOTAL,
            ###########################################################
            "CA_TRANSPORT_OBJECTIF": commercials_objectifs_df.to_dict()[
                "CA TRANSPORT OBJ"
            ][0],
            "CA_TRANSPORT": CA_TRANSPORT,
            ###############################################################
            "MARGE_TRANSPORT_OBJECTIF": commercials_objectifs_df.to_dict()[
                "MARGE TRANSPORT OBJ"
            ][0],
            "MARGE_TRANSPORT": MARGE_TRANSPORT,
            ###############################################################
            "PMV_GLOBAL_OBJECTIF": commercials_objectifs_df.to_dict()["PMV GLOBAL OBJ"][
                0
            ],
            "PMV_GLOBAL": PMV_GLOBAL,
            ##############################################################################
            "CREANCE_COMMERCIAL_OBJECTIF": commercials_objectifs_df.to_dict()[
                "CREANCE COMMERCIAL OBJ"
            ][0],
            "CREANCE_COMMERCIAL": prepare_performance_créance_commerciale_recouvrement(
                target_file, debut_date, fin_date
            )["GRAPHPERFOCECREANCECOMMERCIALE"][-1],
            #################################################################################
            "CREANCE_CRJ_OBJECTIF": commercials_objectifs_df.to_dict()[
                "CREANCE CRJ OBJ"
            ][0],
            "CREANCE_CRJ": prepare_performance_créance_commerciale_recouvrement(
                target_file, debut_date, fin_date
            )["GRAPHCREANCECRJ"][-1],
            ##################################################################################
            "CREANCE_H.RECOUVREMENT_OBJECTIF": commercials_objectifs_df.to_dict()[
                "CREANCE H.RECOUVREMENT OBJ"
            ][0],
            "CREANCE_H.RECOUVREMENT": prepare_performance_créance_commerciale_recouvrement(
                target_file, debut_date, fin_date
            )[
                "GRAPHCREANCEHRECOUVREMENT"
            ][
                -1
            ],
            ###############################################################################
            "CREANCE_CONTENTIEUX_OBJECTIF": commercials_objectifs_df.to_dict()[
                "CREANCE CONTENTIEUX OBJ"
            ][0],
            "CREANCE_CONTENTIEUX": prepare_performance_créance_commerciale_recouvrement(
                target_file, debut_date, fin_date
            )["GRAPHCREANCECONTENIEUX"][-1],
            ###############################################################################
            "CREANCE_GLOBAL_OBJECTIF": commercials_objectifs_df.to_dict()[
                "CREANCE GLOBAL OBJ"
            ][0],
            "CREANCE_GLOBAL": (
                prepare_performance_créance_commerciale_recouvrement(
                    target_file, debut_date, fin_date
                )["GRAPHPERFOCECREANCECOMMERCIALE"][-1]
                + prepare_performance_créance_commerciale_recouvrement(
                    target_file, debut_date, fin_date
                )["GRAPHCREANCECRJ"][-1]
                + prepare_performance_créance_commerciale_recouvrement(
                    target_file, debut_date, fin_date
                )["GRAPHCREANCEHRECOUVREMENT"][-1]
                + prepare_performance_créance_commerciale_recouvrement(
                    target_file, debut_date, fin_date
                )["GRAPHCREANCECONTENIEUX"][-1]
            ),
            ###############################################################################################
            "PMV_NOBLES_OBJECTIF": commercials_objectifs_df.to_dict()["PMV NOBLES OBJ"][
                0
            ],
            "PMV_NOBLES": sum(
                prepare_pmv_data(filtered_ventes, group_by_month)["PMVNOBLES"]
            )
            / len(prepare_pmv_data(filtered_ventes, group_by_month)["PMVNOBLES"]),
            ###############################################################################################
            "PMV_GRAVES_OBJECTIF": commercials_objectifs_df.to_dict()["PMV GRAVES OBJ"][
                0
            ],
            "PMV_GRAVES": sum(
                prepare_pmv_data(filtered_ventes, group_by_month)["PMVGRAVES"]
            )
            / len(prepare_pmv_data(filtered_ventes, group_by_month)["PMVGRAVES"]),
            ###############################################################################################
            ###############################################################################################
            "PMV_STERILE_OBJECTIF": commercials_objectifs_df.to_dict()[
                "PMV STERILE OBJ"
            ][0],
            "PMV STERILE": sum(
                prepare_pmv_data(filtered_ventes, group_by_month)["PMVSTERILE"]
            )
            / len(prepare_pmv_data(filtered_ventes, group_by_month)["PMVSTERILE"]),
            ###############################################################################################
            "RECOUVREMENT_OBJECTIF": commercials_objectifs_df.to_dict()[
                "RECOUVREMENT OBJ"
            ][0],
            "RECOUVREMENT": sum(RECOUVREMENT),
            ###################################################################################################
            "ENCAISSEMENT  OBJECTIF": commercials_objectifs_df.to_dict()[
                "ENCAISSEMENT OBJ"
            ][0],
            "ENCAISSEMENT_FINANCIER": sum(
                prepare_performance_créance_commerciale_recouvrement(
                    target_file, debut_date, fin_date
                )["GRAPHENCAISSEMENTFINANCIER"]
            ),
            ####################################################################################################
            "COMPENSATION_OBJECTIF": commercials_objectifs_df.to_dict()[
                "COMPENSATION OBJ"
            ][0],
            "COUT_TRANSPORT": COUT_TRANSPORT,
        }

        # Prepare final response
        final_response = {
            "Message": "Balance Sheet Generated Successfully",
            "Metrics": metrics_data,
            "TABLES_DATA_OBJECTIFS": Table_Objectifs_DATA,
            "AggregationType": "monthly" if group_by_month else "daily",
            **chart_data,
        }
        print(type((final_response)))
        return final_response

    except Exception as e:
        import traceback

        print(f"Error in balance_sheet function: {str(e)}")
        print(f"Stacktrace: {traceback.format_exc()}")
        return (
            jsonify(
                {
                    "Message": "An error occurred during processing.",
                    "Error": str(e),
                    "Stacktrace": traceback.format_exc(),
                }
            ),
            500,
        )
