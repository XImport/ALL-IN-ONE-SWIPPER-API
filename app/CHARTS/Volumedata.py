from ..Functions.Utils import aggregate_time_series, should_aggregate_monthly


def prepare_volume_data(filtered_data, group_by_month):
    """Prepare volume-related chart data."""
    volume_data = aggregate_time_series(
        filtered_data, "Date", ["Qté en T", "Qté en m3"], group_by_month
    )

    return {
        "GRAPHVOLDATES": volume_data["Date"].tolist(),
        "GRAPHVOLQNTENT": volume_data["Qté en T"].tolist(),
        "GRAPHVOLQNTENM3": volume_data["Qté en m3"].tolist(),
    }
