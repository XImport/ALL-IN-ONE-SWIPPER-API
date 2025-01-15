from ..Functions.Utils import aggregate_time_series, List_Division


def prepare_pmv_data(filtered_data, group_by_month):
    """Prepare PMV-related chart data."""

    def aggregate_by_type(df, type_name):
        type_data = df[df["Type"] == type_name]
        aggregated = aggregate_time_series(
            type_data, "Date", ["CA Net", "Qté en T"], group_by_month
        )

        # Assuming List_Division takes two lists and returns some structured data
        return List_Division(
            aggregated["CA Net"].tolist(), aggregated["Qté en T"].tolist()
        )

    # Since aggregate_time_series now returns just a list of dates,
    # you can directly assign it to "GRAPHDATEPMV"

    dates = aggregate_time_series(
        filtered_data, "Date", ["CA Net", "Qté en T"], group_by_month
    )

    print("result hereeeeeee", dates)
    return {
        "PMVDATES": dates["Date"].tolist(),
        "PMVNOBLES": aggregate_by_type(filtered_data, "Nobles"),
        "PMVGRAVES": aggregate_by_type(filtered_data, "Graves"),
        "PMVSTERILE": aggregate_by_type(filtered_data, "Stérile"),
    }
