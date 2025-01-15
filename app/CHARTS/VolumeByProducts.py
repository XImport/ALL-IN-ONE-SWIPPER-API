def prepare_volume_data_by_product(filtered_data, group_by_month):
    """Prepare volume data grouped by product over time."""
    # First ensure we're working with a copy to avoid modifying original data
    df = filtered_data.copy()

    # Calculate total volume
    total_volume = df["Qté en T"].sum()

    # Group by product to get overall totals
    product_totals = df.groupby("Produit")["Qté en T"].sum().reset_index()

    # Calculate percentages
    product_totals["Percentage"] = (
        product_totals["Qté en T"] / total_volume * 100
    ).round(2)

    return {
        "PRODUITS": product_totals["Produit"].tolist(),
        "QNTBYPRODUIT": product_totals["Percentage"].tolist(),
    }
