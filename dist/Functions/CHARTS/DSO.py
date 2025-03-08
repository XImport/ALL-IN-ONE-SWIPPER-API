def calculate_DSO_clients(CA_DF, Creance_Client_DF):
    # Group CA_DF by month and sum the CA BRUT
    CA_monthly = CA_DF.groupby(CA_DF['Date'].dt.to_period('M'))['CA BRUT'].sum().reset_index()
    
    
    return CA_monthly
