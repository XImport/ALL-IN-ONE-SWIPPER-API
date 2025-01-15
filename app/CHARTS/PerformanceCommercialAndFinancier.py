import pandas as pd


def prepare_performance_créance_commerciale_recouvrement(
    SourceFile, Start_date, End_date
):
    Etat_financier = pd.read_excel(SourceFile, sheet_name="ETAT FINANCIER")

    Etat_financier["Date"] = pd.to_datetime(
        Etat_financier["Date"], format="%d/%m/%Y", errors="coerce"
    )

    Etat_financier["Formatted_Date"] = Etat_financier["Date"].dt.strftime("%d/%m/%Y")

    Search_Start_Month = Start_date.month
    Search_End_Month = End_date.month

    # Handle cases where end month is less than start month (crossing year boundary)
    if Search_End_Month < Search_Start_Month:
        filtered_rows = Etat_financier[
            (
                (Etat_financier["Date"].dt.month >= Search_Start_Month)
                | (Etat_financier["Date"].dt.month <= Search_End_Month)
            )
        ]
    else:
        filtered_rows = Etat_financier[
            (Etat_financier["Date"].dt.month >= Search_Start_Month)
            & (Etat_financier["Date"].dt.month <= Search_End_Month)
        ]

    return {
        "GRAPHPERFOCECREANCECOMMERCIALEDATES": filtered_rows[
            "Formatted_Date"
        ].to_list(),
        "GRAPHPERFOCECREANCECOMMERCIALE": filtered_rows[
            "Créance Commerciale"
        ].to_list(),
        "GRAPHRECOUVREMENTCOMMERCIAL": filtered_rows[
            "Recouvrement Commerciale"
        ].to_list(),
        "GRAPHENCAISSEMENTFINANCIER": filtered_rows["Encaissement Financier"].to_list(),
        "GRAPHCREANCECRJ": filtered_rows["Créance CRJ"].to_list(),
        "GRAPHCREANCEHRECOUVREMENT": filtered_rows["Créance H.RECOUVREMENT"].to_list(),
        "GRAPHCREANCECONTENIEUX": filtered_rows["Créance CONTENTIEUX"].to_list(),
    }
