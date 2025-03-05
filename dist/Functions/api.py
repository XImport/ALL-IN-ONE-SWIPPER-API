from flask import jsonify, request, Response, Blueprint

import os
import pandas as pd
from datetime import datetime
from .Utilitys.Utils import (get_files_in_directory, should_aggregate_monthly,
                             aggregate_time_series, Metrics_DATA_Filters,
                             prepare_recouvrement_data,
                             process_client_products, should_keep_product)
from .CHARTS.Volumedata import prepare_volume_data
from .CHARTS.VolumeByProducts import prepare_volume_data_by_product
from .CHARTS.CANetByProducts import prepare_ca_net_by_product
from .CHARTS.VoyagesRendus import prepare_voyages_rendus_data
from .CHARTS.CANetandCABrut import prepare_ca_data
from .CHARTS.VolumeDataByProductByDates import prepare_volume_data_by_product_by_dates
from .CHARTS.PerformanceCommercialAndFinancier import (
    prepare_performance_créance_commerciale_recouvrement, )
from .CHARTS.PMVGlobal import prepare_pmv_data
from .CHARTS.MargeBeneficiare import process_marge_products, calculate_marge
from .CHARTS.TopSixClients import prepare_top_six_clients
from .CHARTS.DSO import calculate_client_DSO
from .CHARTS.CreanceVSCA import CreanceVsCA
from flask_cors import CORS, cross_origin
import concurrent.futures
from functools import lru_cache
import json

main = Blueprint("main", __name__)


def Metrics(filtered_data, group_by_month, args, df_recouvrement, debut_date,
            fin_date):
    RECOUVREMENT_DATA = df_recouvrement
    RECOUVREMENT_DATA["Date de Paiement"] = pd.to_datetime(
        RECOUVREMENT_DATA["Date de Paiement"],
        format="%d/%m/%Y",
        errors="coerce")
    RECOUVREMENT_DATA = RECOUVREMENT_DATA[
        (RECOUVREMENT_DATA["Date de Paiement"].dt.date >= debut_date.date())
        & (RECOUVREMENT_DATA["Date de Paiement"].dt.date <= fin_date.date())]

    RECOUVREMENT_DATA["Date de Paiement"] = RECOUVREMENT_DATA[
        "Date de Paiement"].dt.strftime("%d/%m/%Y")
    En_espece_filtered = filtered_data[filtered_data["BC"] ==
                                       "EN ESPECE"].copy()
    nobles_filtered = filtered_data[filtered_data["Type"] == "Nobles"].copy()
    graves_filtered = filtered_data[filtered_data["Type"] == "Graves"].copy()
    steriles_filtered = filtered_data[filtered_data["Type"] ==
                                      "Stérile"].copy()

    CA_NET_NOBLES = Metrics_DATA_Filters(nobles_filtered,
                                         group_by_month)["CA Net"].sum()
    CA_NET_GRAVES = Metrics_DATA_Filters(graves_filtered,
                                         group_by_month)["CA Net"].sum()
    CA_NET_STERILES = Metrics_DATA_Filters(steriles_filtered,
                                           group_by_month)["CA Net"].sum()
    QNT_NET_NOBLES = Metrics_DATA_Filters(nobles_filtered,
                                          group_by_month)["Qté en T"].sum()
    QNT_NET_GRAVES = Metrics_DATA_Filters(graves_filtered,
                                          group_by_month)["Qté en T"].sum()
    QNT_NET_STERILES = Metrics_DATA_Filters(steriles_filtered,
                                            group_by_month)["Qté en T"].sum()

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
            "METRICS_CA_BRUT":
            CA_BRUT_TOTAL,
            "METRICS_CA_NET":
            CA_NET_TOTAL,
            "METRICS_PMV_GLOBAL":
            PMV_GLOBAL,
            "METRICS_QNT_EN_TONNE_GLOBALE":
            QNT_EN_TONNE_TOTAL,
            "METRICS_PMV_HORS_STERILE":
            ((CA_NET_NOBLES + CA_NET_GRAVES) /
             (QNT_NET_NOBLES + QNT_NET_GRAVES) if
             (QNT_NET_NOBLES + QNT_NET_GRAVES) != 0 else 0),
            "METRICS_MARGE_TRANSPORT":
            MARGE_TRANSPORT,
        }
    elif args == "METRICS#2":
        MIX_PRODUCT = (QNT_NET_NOBLES /
                       QNT_EN_TONNE_TOTAL if QNT_EN_TONNE_TOTAL != 0 else 0)

        CAISSE_ESPECE = Metrics_DATA_Filters(En_espece_filtered,
                                             group_by_month)["CA BRUT"].sum()
        VOYAGES_RENDUS = prepare_voyages_rendus_data(filtered_data,
                                                     group_by_month)
        global RECOUVREMENT
        RECOUVREMENT = RECOUVREMENT_DATA["Montant Paye"].tolist()
        return {
            "MIX_PRODUCT": MIX_PRODUCT,
            "CAISSE_ESPECE": CAISSE_ESPECE,
            "VOYAGES_RENDUS": sum(VOYAGES_RENDUS["GRAPHVOYAGERENDULIVREE"]),
            "RECOUVREMENT_EFFECTUER": sum(RECOUVREMENT),
        }


@main.route("/", methods=["GET"])
@cross_origin()
def Home():
    return jsonify({"response": "Home Response"})


@main.route("/API/V1/TESTIGN", methods=["GET"])
@cross_origin()
def Testing():
    return jsonify({"response": "hello world"})


# Add caching for file reading
@lru_cache(maxsize=32)
def read_excel_file(file_path):
    return {
        "ventes": pd.read_excel(file_path, sheet_name="VENTES"),
        "recouvrement": pd.read_excel(file_path, sheet_name="RECOUVREMENT"),
        "objectifs": pd.read_excel(file_path, sheet_name="OBJECTIFS"),
        "info_clients": pd.read_excel(file_path, sheet_name="INFO CLIENTS"),
        "cout_revien": pd.read_excel(file_path, sheet_name="COUT REVIEN"),
        "creance_client": pd.read_excel(file_path, sheet_name="CREANCES"),
    }


# Parallelize data preparation functions
def prepare_data_parallel(filtered_ventes, group_by_month, target_file,
                          debut_date, fin_date):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            "volume_data":
            executor.submit(prepare_volume_data, filtered_ventes,
                            group_by_month),
            "ca_data":
            executor.submit(prepare_ca_data, filtered_ventes, group_by_month),
            "pmv_data":
            executor.submit(prepare_pmv_data, filtered_ventes, group_by_month),
            "voyages_data":
            executor.submit(prepare_voyages_rendus_data, filtered_ventes,
                            group_by_month),
            "volume_by_product":
            executor.submit(prepare_volume_data_by_product, filtered_ventes,
                            group_by_month),
            "ca_net_by_product":
            executor.submit(prepare_ca_net_by_product, filtered_ventes,
                            group_by_month),
            "top_six_clients":
            executor.submit(prepare_top_six_clients, filtered_ventes,
                            group_by_month),
            "performance_creance":
            executor.submit(
                prepare_performance_créance_commerciale_recouvrement,
                target_file,
                debut_date,
                fin_date,
            ),
        }

        return {
            "VOLGRAPH": futures["volume_data"].result(),
            "CAGRAPH": futures["ca_data"].result(),
            "PMVGRAPH": futures["pmv_data"].result(),
            "COMMANDEGRAPH": futures["voyages_data"].result(),
            "QNTBYPRODUITGRAPH": futures["volume_by_product"].result(),
            "CANETBYPRODUITGRAPH": futures["ca_net_by_product"].result(),
            "TOP6CLIENTSGRAPH": futures["top_six_clients"].result(),
            "PERFORMANCECREANCEGRAPH": futures["performance_creance"].result(),
        }


@main.route("/API/V1/BalanceSheet", methods=["GET", "POST"])
@cross_origin()
def balance_sheet():
    try:
        # Get and validate input dates
        debut_date = request.json.get("DébutDate")
        fin_date = request.json.get("FinDate")

        if not debut_date or not fin_date:
            return jsonify({"Message":
                            "DébutDate and FinDate are required."}), 400

        # Convert dates once and reuse
        try:
            debut_date = pd.to_datetime(debut_date, format="%d/%m/%Y")
            fin_date = pd.to_datetime(fin_date, format="%d/%m/%Y")
        except ValueError:
            return jsonify({"Message":
                            "Invalid date format. Use DD/MM/YYYY."}), 400

        if debut_date.year != fin_date.year:
            return jsonify({
                "Message":
                "Date de début et date de fin doivent être dans la même année"
            }), 400

        if fin_date < debut_date:
            return (
                jsonify(
                    {"Message": "FinDate cannot be earlier than DébutDate."}),
                404,
            )

        # File path handling
        year = str(debut_date.year)
        source_path = os.path.join(os.path.dirname(__file__), "Source", year)
        target_file = os.path.join(source_path, f"Source {year}.xlsx")

        # Validate file existence
        if not os.path.exists(source_path) or not os.path.exists(target_file):
            return jsonify({"Message": f"Data not found for year {year}"}), 404

        # data = read_excel_file(target_file)
        # ventes_df = data["ventes"]
        # if ventes_df["Date"].iloc[-1] < fin_date:
        #     return jsonify({
        #         "Message":
        #         "Les données recherchées ne sont pas accessibles."
        #     }), 200

        # Read data with caching

        try:
            data = read_excel_file(target_file)
            ventes_df = data["ventes"]

            recouvrement_df = data["recouvrement"]
            commercials_objectifs_df = data["objectifs"]
        except Exception as e:
            return jsonify({"Message": f"Error reading data: {str(e)}"}), 500

        # Optimize date handling

        # Convert the Date column to datetime
        ventes_df["Date"] = pd.to_datetime(ventes_df["Date"],
                                           format="%d/%m/%Y",
                                           errors="coerce")

        recouvrement_df["Date de Paiement"] = pd.to_datetime(
            recouvrement_df["Date de Paiement"],
            format="%d/%m/%Y",
            errors="coerce")

        # Use vectorized operations for filtering
        date_mask_ventes = (ventes_df["Date"].dt.date >= debut_date.date()) & (
            ventes_df["Date"].dt.date <= fin_date.date())
        date_mask_recouvrement = (
            recouvrement_df["Date de Paiement"].dt.date >= debut_date.date()
        ) & (recouvrement_df["Date de Paiement"].dt.date <= fin_date.date())

        filtered_ventes = ventes_df[date_mask_ventes]
        filtered_recouvrement = recouvrement_df[date_mask_recouvrement]

        if filtered_ventes.empty and filtered_recouvrement.empty:
            return (
                jsonify(
                    {"Message": "No data found between the specified dates."}),
                404,
            )

        # Determine aggregation type
        group_by_month = should_aggregate_monthly(debut_date, fin_date)

        # Parallel processing for chart data
        chart_data = prepare_data_parallel(filtered_ventes, group_by_month,
                                           target_file, debut_date, fin_date)

        # Calculate metrics in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            metrics_one = executor.submit(
                Metrics,
                filtered_ventes,
                group_by_month,
                "METRICS#1",
                filtered_recouvrement,
                debut_date,
                fin_date,
            )
            metrics_two = executor.submit(
                Metrics,
                filtered_ventes,
                group_by_month,
                "METRICS#2",
                filtered_recouvrement,
                debut_date,
                fin_date,
            )

        metrics_data = {
            "METRICS_ONE": metrics_one.result(),
            "METRICS_TWO": metrics_two.result(),
        }

        # Prepare final response
        final_response = {
            "Metrics":
            metrics_data,
            "TABLES_DATA_OBJECTIFS":
            prepare_objectives_data(
                commercials_objectifs_df,
                filtered_ventes,
                filtered_recouvrement,
                target_file,
                debut_date,
                fin_date,
            ),
            "AggregationType":
            "monthly" if group_by_month else "daily",
            **chart_data,
        }

        return jsonify(final_response)

    except Exception as e:
        return jsonify({"Message": "An error occurred", "Error": str(e)}), 500


def prepare_objectives_data(
    objectifs_df,
    filtered_ventes,
    filtered_recouvrement,
    target_file,
    debut_date,
    fin_date,
):
    """
    Prepare objectives data comparing actual values against targets.

    Args:
        objectifs_df: DataFrame containing objective/target values
        filtered_ventes: DataFrame containing filtered sales data
        filtered_recouvrement: DataFrame containing filtered recovery data
        target_file: Path to the source Excel file
        debut_date: Start date for the period
        fin_date: End date for the period

    Returns:
        dict: Dictionary containing actual vs objective values
    """
    # Extract objectives once
    objectives = objectifs_df.to_dict()

    # Calculate basic metrics
    ca_brut_total = filtered_ventes["CA BRUT"].sum()
    ca_net_total = filtered_ventes["CA Net"].sum()
    ca_transport = filtered_ventes["CA Transport"].sum()
    marge_transport = filtered_ventes["Marge sur Transport"].sum()
    cout_transport = filtered_ventes["Coût de transport"].sum()

    # Calculate PMV (Prix Moyen de Vente) Global
    qnt_en_tonne_total = filtered_ventes["Qté en T"].sum()
    pmv_global = ca_net_total / qnt_en_tonne_total if qnt_en_tonne_total != 0 else 0

    # Calculate PMV by product type
    def calculate_pmv_by_type(product_type):
        product_data = filtered_ventes[filtered_ventes["Type"] == product_type]
        sales = product_data["CA Net"].sum()
        quantity = product_data["Qté en T"].sum()
        return sales / quantity if quantity != 0 else 0

    pmv_nobles = calculate_pmv_by_type("Nobles")
    pmv_graves = calculate_pmv_by_type("Graves")
    pmv_sterile = calculate_pmv_by_type("Stérile")

    # Calculate recovery metrics
    recouvrement_total = filtered_recouvrement["Montant Paye"].sum()

    # Get performance creance data
    perf_creance = prepare_performance_créance_commerciale_recouvrement(
        target_file, debut_date, fin_date)

    # Prepare the response dictionary
    return {
        # CA (Chiffre d'Affaires) Metrics
        "CA_BRUT_OBJECTIF":
        objectives["CA BRUT OBJ"][0],
        "CA_BRUT":
        ca_brut_total,
        "CA_NET_OBJECTIF":
        objectives["CA NET OBJ"][0],
        "CA_NET":
        ca_net_total,
        "CA_TRANSPORT_OBJECTIF":
        objectives["CA TRANSPORT OBJ"][0],
        "CA_TRANSPORT":
        ca_transport,
        # Transport Margins
        "MARGE_TRANSPORT_OBJECTIF":
        objectives["MARGE TRANSPORT OBJ"][0],
        "MARGE_TRANSPORT":
        marge_transport,
        # PMV (Prix Moyen de Vente) Metrics
        "PMV_GLOBAL_OBJECTIF":
        objectives["PMV GLOBAL OBJ"][0],
        "PMV_GLOBAL":
        pmv_global,
        # Creance (Receivables) Metrics
        "CREANCE_COMMERCIAL_OBJECTIF":
        objectives["CREANCE COMMERCIAL OBJ"][0],
        "CREANCE_COMMERCIAL":
        perf_creance["GRAPHPERFOCECREANCECOMMERCIALE"][-1],
        "CREANCE_CRJ_OBJECTIF":
        objectives["CREANCE CRJ OBJ"][0],
        "CREANCE_CRJ":
        perf_creance["GRAPHCREANCECRJ"][-1],
        "CREANCE_H.RECOUVREMENT_OBJECTIF":
        objectives["CREANCE H.RECOUVREMENT OBJ"][0],
        "CREANCE_H.RECOUVREMENT":
        perf_creance["GRAPHCREANCEHRECOUVREMENT"][-1],
        "CREANCE_CONTENTIEUX_OBJECTIF":
        objectives["CREANCE CONTENTIEUX OBJ"][0],
        "CREANCE_CONTENTIEUX":
        perf_creance["GRAPHCREANCECONTENIEUX"][-1],
        # Calculate Global Creance
        "CREANCE_GLOBAL_OBJECTIF":
        objectives["CREANCE GLOBAL OBJ"][0],
        "CREANCE_GLOBAL": (perf_creance["GRAPHPERFOCECREANCECOMMERCIALE"][-1] +
                           perf_creance["GRAPHCREANCECRJ"][-1] +
                           perf_creance["GRAPHCREANCEHRECOUVREMENT"][-1] +
                           perf_creance["GRAPHCREANCECONTENIEUX"][-1]),
        # PMV by Product Type
        "PMV_NOBLES_OBJECTIF":
        objectives["PMV NOBLES OBJ"][0],
        "PMV_NOBLES":
        pmv_nobles,
        "PMV_GRAVES_OBJECTIF":
        objectives["PMV GRAVES OBJ"][0],
        "PMV_GRAVES":
        pmv_graves,
        "PMV_STERILE_OBJECTIF":
        objectives["PMV STERILE OBJ"][0],
        "PMV_STERILE":
        pmv_sterile,
        # Recovery and Financial Metrics
        "RECOUVREMENT_OBJECTIF":
        objectives["RECOUVREMENT OBJ"][0],
        "RECOUVREMENT":
        recouvrement_total,
        "ENCAISSEMENT_OBJECTIF":
        objectives["ENCAISSEMENT OBJ"][0],
        "ENCAISSEMENT_FINANCIER":
        sum(perf_creance["GRAPHENCAISSEMENTFINANCIER"]),
        "COMPENSATION_OBJECTIF":
        objectives["COMPENSATION OBJ"][0],
        "COUT_TRANSPORT":
        cout_transport,
    }


@main.route("/API/V1/InfoClients", methods=["POST"])
@cross_origin()
def Info_Clients_req():
    try:
        # Get and validate input dates
        debut_date = request.json.get("DébutDate")
        fin_date = request.json.get("FinDate")

        if not debut_date or not fin_date:
            return jsonify({"Message":
                            "DébutDate and FinDate are required."}), 400

        # Convert dates once and reuse
        try:
            debut_date = pd.to_datetime(debut_date, format="%d/%m/%Y")
            fin_date = pd.to_datetime(fin_date, format="%d/%m/%Y")
        except ValueError:
            return jsonify({"Message":
                            "Invalid date format. Use DD/MM/YYYY."}), 400

        if debut_date.year != fin_date.year:
            return jsonify({
                "Message":
                "Date de début et date de fin doivent être dans la même année"
            }), 400

        if fin_date < debut_date:
            return (
                jsonify({
                    "Message":
                    "La date de fin ne peut pas être antérieure à la date de début."
                }),
                400,
            )

        # File path handling

        year = str(debut_date.year)
        source_path = os.path.join(os.path.dirname(__file__), "Source", year)
        target_file = os.path.join(source_path, f"Source {year}.xlsx")

        # Validate file existence
        if not os.path.exists(source_path) or not os.path.exists(target_file):
            return jsonify({"Message": f"Data not found for year {year}"}), 404

        # Read data with caching

        data = read_excel_file(target_file)
        ventes_df = data["ventes"]
        if ventes_df["Date"].iloc[-1] < fin_date:
            return jsonify({
                "Message":
                "Les données recherchées ne sont pas accessibles."
            }), 200

        try:
            data = read_excel_file(target_file)
            info_clients_df = data["info_clients"]
            ventes_df = data["ventes"]
        except Exception as e:
            return jsonify({"Message": f"Error reading data: {str(e)}"}), 500

        date_mask_ventes = (ventes_df["Date"].dt.date >= debut_date.date()) & (
            ventes_df["Date"].dt.date <= fin_date.date())
        info_clients_df = info_clients_df.map(
            lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)

        # info_clients_json = json.loads(info_clients_df.to_json(orient="records"))

        # First get the matched records
        filtered_ventes = ventes_df[date_mask_ventes]
        info_clients = info_clients_df["NOM DU CLIENT"].str.strip().str.upper()
        ventes_clients = filtered_ventes["Client"].str.strip().str.upper()
        matching_clients_mask = ventes_clients.isin(info_clients)
        matching_records = filtered_ventes[matching_clients_mask]

        # Convert info_clients_df to records
        client_records = info_clients_df.to_dict(orient="records")

        # Create a dictionary of total quantities per client
        client_quantities = (
            matching_records.groupby("Client")["Qté en T"].sum().to_dict())

        client_CA_BRUT = matching_records.groupby(
            "Client")["CA BRUT"].sum().to_dict()
        client_Cout_Transport = (matching_records.groupby("Client")
                                 ["Coût de transport"].sum().to_dict())

        # Add quantities to each client record
        for record in client_records:
            client_name = record["NOM DU CLIENT"].strip().upper()
            record["Qté en T"] = client_quantities.get(client_name, 0)
            record["CA BRUT"] = client_CA_BRUT.get(client_name, 0)
            record["COUT TRANSPORT"] = client_Cout_Transport.get(
                client_name, 0)

        # Return the array of objects directly (no need for json.dumps)

        return jsonify({"INFO_CLIENTS": client_records})

    except Exception as e:
        return jsonify({"Message": "An error occurred", "Error": str(e)}), 500


@main.route("/API/V1/AnalyseClient", methods=["GET", "POST"])
@cross_origin()
def AnalyseClient():
    try:
        # Get and validate input dates
        debut_date = request.json.get("DébutDate")
        fin_date = request.json.get("FinDate")

        # Get client filter (optional) - can be a single client name or a list of clients
        clients = request.json.get("Clients", [])

        # Get chart elements to exclude (optional)
        exclude_charts = request.json.get("ExcludeCharts", [])
        if isinstance(exclude_charts, str):
            exclude_charts = [exclude_charts]

        # Convert single client to list for consistent handling
        if isinstance(clients, str):
            clients = [clients]

        if not debut_date or not fin_date:
            return jsonify({"Message":
                            "DébutDate and FinDate are required."}), 400

        # Convert dates once and reuse
        try:
            debut_date = pd.to_datetime(debut_date, format="%d/%m/%Y")
            fin_date = pd.to_datetime(fin_date, format="%d/%m/%Y")
        except ValueError:
            return jsonify({"Message":
                            "Invalid date format. Use DD/MM/YYYY."}), 400

        if debut_date.year != fin_date.year:
            return jsonify({
                "Message":
                "Date de début et date de fin doivent être dans la même année"
            }), 400

        if fin_date < debut_date:
            return (
                jsonify(
                    {"Message": "FinDate cannot be earlier than DébutDate."}),
                404,
            )

        # File path handling
        year = str(debut_date.year)
        source_path = os.path.join(os.path.dirname(__file__), "Source", year)
        target_file = os.path.join(source_path, f"Source {year}.xlsx")

        # Validate file existence
        if not os.path.exists(source_path) or not os.path.exists(target_file):
            return jsonify({"Message": f"Data not found for year {year}"}), 404

        try:
            data = read_excel_file(target_file)
            ventes_df = data["ventes"]
            recouvrement_df = data["recouvrement"]
            creance_client_df = data["creance_client"]
            cout_revien_df = data["cout_revien"]
            info_clients_df = data["info_clients"]

        except Exception as e:
            return jsonify({"Message": f"Error reading data: {str(e)}"}), 500

        # Convert the Date column to datetime
        ventes_df["Date"] = pd.to_datetime(ventes_df["Date"],
                                           format="%d/%m/%Y",
                                           errors="coerce")

        recouvrement_df["Date de Paiement"] = pd.to_datetime(
            recouvrement_df["Date de Paiement"],
            format="%d/%m/%Y",
            errors="coerce")

        # Use vectorized operations for filtering by date
        date_mask_ventes = (ventes_df["Date"].dt.date >= debut_date.date()) & (
            ventes_df["Date"].dt.date <= fin_date.date())
        date_mask_recouvrement = (
            recouvrement_df["Date de Paiement"].dt.date >= debut_date.date()
        ) & (recouvrement_df["Date de Paiement"].dt.date <= fin_date.date())

        filtered_ventes = ventes_df[date_mask_ventes]
        filtered_recouvrement = recouvrement_df[date_mask_recouvrement]

        # Create a dictionary to map clients to their delay days
        client_delay_days = {}
        for client in clients:
            info_clients_df_client = info_clients_df[
                info_clients_df["NOM DU CLIENT"] == client]
            client_row = info_clients_df_client.to_dict(
                'records')[0] if not info_clients_df_client.empty else None

            if client_row["MODE DE REGLEMENT"].lower(
            ) == "en avance":  # Make case-insensitive
                client_delay_days[client] = 0
            else:
                client_delay_days[client] = int(
                    client_row["MODE DE REGLEMENT"].split(" ")[0])

        DSO_clients = calculate_client_DSO(
            creance_client_df[creance_client_df["Client"].isin(clients)],
            client_delay_days)

        # Filter creance_df_mask by clients
    

        # Apply client filtering to both dataframes passed to CreanceVsCA
        Creance_client = CreanceVsCA(
            filtered_ventes[filtered_ventes["Client"].isin(clients)],
                        creance_client_df,clients,fin_date)
        print("response",Creance_client)
        # Apply client filtering if clients list is not empty
        QNT_BY_PRODUCTS_GRAPH = {
            "GRAPHDATES": [],
            "GRAPHLABELPRODUCTS": [],
            "GRAPHDATAPRODUCTS": []
        }
        # ////////////////// prix de vente
        GRAPHCOUTREVIEN = {
            "PRODUCTSNAME": [],
            "COUTREVIEN": [],
            "PRIXVENTE": [],
            "UNITE": []
        }

        if clients:
            # Assuming the client column in ventes_df is named "Client"
            filtered_ventes = filtered_ventes[filtered_ventes["Client"].isin(
                clients)]

            # Assuming the client column in recouvrement_df is named "Client"
            filtered_recouvrement = filtered_recouvrement[
                filtered_recouvrement["Client"].isin(clients)]

        if filtered_ventes.empty and filtered_recouvrement.empty:
            return (
                jsonify({
                    "Message":
                    "No data found between the specified dates and client filters."
                }),
                404,
            )

        # Determine aggregation type

        group_by_month = should_aggregate_monthly(debut_date, fin_date)
        print(
            "yeeeeeees",
            process_client_products(clients, info_clients_df, cout_revien_df))

        # Parallel processing for chart data
        chart_data = prepare_data_parallel(filtered_ventes, group_by_month,
                                           target_file, debut_date, fin_date)

        # Add recouvrement chart data
        try:
            recouvrement_chart = prepare_recouvrement_data(
                filtered_recouvrement, group_by_month)
            chart_data["RECOUVREMENTGRAPH"] = recouvrement_chart
        except Exception as e:
            print(f"Error preparing recouvrement chart: {str(e)}")
            # Add default empty recouvrement chart structure
            chart_data["RECOUVREMENTGRAPH"] = {"DATES": [], "MONTANTS": []}
        # Check if PMVGRAPH exists, if not add it with default structure
        if "PMVGRAPH" not in chart_data:
            # Generate default date range based on filter dates
            start_month = debut_date.replace(day=1)
            end_month = fin_date.replace(day=1)

            # Create list of months in MM/YYYY format
            dates = []
            current = start_month
            while current <= end_month:
                dates.append(current.strftime("%m/%Y"))
                # Move to next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)

            # Create default PMV structure with zeros
            chart_data["PMVGRAPH"] = {
                "PMVDATES": dates,
                "PMVNOBLES": [0.0] * len(dates),
                "PMVGRAVES": [0.0] * len(dates),
                "PMVSTERILE": [0.0] * len(dates)
            }
            chart_data.update({
                "CHARTCOUTREVIENWITHPRIXVENTE":
                process_client_products(clients, info_clients_df,
                                        cout_revien_df)
            })

        # Default exclusions - fixed keys to match the actual keys in chart_data
        default_exclusions = [
            "PERFORMANCECREANCEGRAPH", "TOP6CLIENTSGRAPH", "COMMANDEGRAPH"
        ]

        # Add default exclusions to the user-provided exclusions
        all_exclusions = exclude_charts + default_exclusions

        # Remove unwanted chart elements
        for chart_key in all_exclusions:
            if chart_key in chart_data:
                del chart_data[chart_key]

        # Calculate metrics in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            metrics_one = executor.submit(
                Metrics,
                filtered_ventes,
                group_by_month,
                "METRICS#1",
                filtered_recouvrement,
                debut_date,
                fin_date,
            )
            metrics_two = executor.submit(
                Metrics,
                filtered_ventes,
                group_by_month,
                "METRICS#2",
                filtered_recouvrement,
                debut_date,
                fin_date,
            )

        metrics_data = {
            "METRICS_ONE": metrics_one.result(),
            "METRICS_TWO": metrics_two.result(),
        }

        # Prepare final response
        final_response = {
            "Metrics":
            metrics_data,
            "VOLUMEDATABYPRODUCTSBYDATES":
            prepare_volume_data_by_product_by_dates(filtered_ventes,
                                                    group_by_month),
            "AggregationType":
            "monthly" if group_by_month else "daily",
            "ClientsFiltered":
            clients if clients else "All",
            "ExcludedCharts":
            all_exclusions,
            "MARGE_PRODUCTS_BY_CLIENTS_CHART":
            process_marge_products(
                clients, info_clients_df, cout_revien_df,
                filtered_ventes["Produit"].unique().tolist()),
            "CHARTCOUTREVIENWITHPRIXVENTE":
            process_client_products(
                clients, info_clients_df, cout_revien_df,
                filtered_ventes["Produit"].unique().tolist()),
            "CREANCE_CLIENT_CHART":
            Creance_client,
            "DSO_CLIENTS_CHART":
            DSO_clients,
            **chart_data,
        }

        return jsonify(final_response)

    except Exception as e:
        return jsonify({"Message": "An error occurred", "Error": str(e)}), 500


@main.route("/API/V1/QueryClients", methods=["GET"])
@cross_origin()
def Query_Clients_DATA():
    try:
        # Define the year and construct file paths
        year = "2025"
        source_path = os.path.join(os.path.dirname(__file__), "Source", year)
        target_file = os.path.join(source_path, f"Source {year}.xlsx")

        # Validate file existence
        if not os.path.exists(source_path) or not os.path.exists(target_file):
            return jsonify({"Message": f"Data not found for year {year}"}), 404

        # Read data
        try:
            data = read_excel_file(target_file)
            info_clients_df = data["info_clients"]
        except Exception as e:
            return jsonify({"Message": f"Error reading data: {str(e)}"}), 500

        # Convert timestamps if needed
        info_clients_df = info_clients_df.map(
            lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)

        # ✅ Rename "NOM DU CLIENT" to "CLIENTNAME" and select required columns
        result = info_clients_df[[
            'CODE', 'NOM DU CLIENT'
        ]].rename(columns={"NOM DU CLIENT": "CLIENTNAME"})

        return jsonify({"INFO_CLIENTS": result.to_dict(orient="records")})

    except Exception as e:
        return jsonify({"Message": "An error occurred", "Error": str(e)}), 500
