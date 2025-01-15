import pandas as pd


def prepare_ca_net_by_product(filtered_data, group_by_month):
    """Prepare CA NET data grouped by product."""
    # First ensure we're working with a copy to avoid modifying original data
    df = filtered_data.copy()

    # Calculate total CA NET
    total_ca_net = df["CA Net"].sum()

    # Group by product to get overall totals
    product_totals = df.groupby("Produit")["CA Net"].sum().reset_index()

    # Calculate percentages
    product_totals["Percentage"] = (
        product_totals["CA Net"] / total_ca_net * 100
    ).round(2)

    return {
        "PRODUITS": product_totals["Produit"].tolist(),
        "CANETBYPRODUIT": product_totals["Percentage"].tolist(),
    }
