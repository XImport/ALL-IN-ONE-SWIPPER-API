def prepare_volume_data_by_product_by_dates(filtered_data, group_by_month):
    """Prepare volume data grouped by product over time."""
    # First ensure we're working with a copy to avoid modifying original data
    df = filtered_data.copy()
    
    # Group by product to get overall totals
    product_totals = df.groupby("Produit")[["Qté en T", "CA Net"]].sum().reset_index()
    
    # Calculate overall totals
    total_quantity = df["Qté en T"].sum()
    total_ca_net = df["CA Net"].sum()
    
    return {
        "PRODUITS": product_totals["Produit"].tolist(),
        "QNTBYPRODUIT": product_totals["Qté en T"].tolist(),
        "CANETBYPRODUIT": product_totals["CA Net"].tolist(),
        "TOTAL_QNT": total_quantity,
        "TOTAL_CANET": total_ca_net
    }