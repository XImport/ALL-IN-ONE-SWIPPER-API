from ..Functions.Utils import aggregate_time_series


def prepare_ca_data(filtered_data, group_by_month):
    """Prepare CA-related chart data."""
    ca_data = aggregate_time_series(
        filtered_data, "Date", ["CA BRUT", "CA Net"], group_by_month
    )

    return {
        "GRAPHCADATESS": ca_data["Date"].tolist(),
        "GRAPHCABRUT": ca_data["CA BRUT"].tolist(),
        "GRAPHCANET": ca_data["CA Net"].tolist(),
    }
