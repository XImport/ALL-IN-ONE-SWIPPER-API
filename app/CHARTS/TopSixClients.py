def prepare_top_six_clients(filtered_data, group_by_month):
    """Prepare top 6 clients data based on CA BRUT."""
    # Group by client and sum CA BRUT
    client_totals = filtered_data.groupby("Client")["CA BRUT"].sum().reset_index()

    # Sort by CA BRUT descending and get top 6
    top_6_clients = client_totals.sort_values(by="CA BRUT", ascending=False).head(6)

    return {
        "TOP6CLIENTNAMES": top_6_clients["Client"].tolist(),
        "TOP6CLIENTVALUES": top_6_clients["CA BRUT"].tolist(),
    }
