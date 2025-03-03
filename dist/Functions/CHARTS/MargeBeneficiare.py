from ..Utilitys.Utils import  List_Division , calculate_marge

def process_marge_products(clients, info_clients_df, cout_revien_df):
    """
    Process client products, handle different units (/T and /M3), and track pricing information.
    
    Args:
        clients: List of client names to process
        info_clients_df: DataFrame containing client information
        cout_revien_df: DataFrame containing cost information with units
        
    Returns:
        Tuple containing (GRAPHCOUTREVIEN, ScopeUNITE) dictionaries
    """
    GRAPHCOUTREVIEN = {"PRODUCTSNAME": [], "COUTREVIEN": [], "PRIXVENTE": [], "UNITE": []}
    ScopeUNITE = {}
    
    # Extract product names from cout_revien_df
    product_columns = cout_revien_df.columns.tolist()[2:]  # Skip first two columns
    
    # Get the rows for cost per ton and cost per cubic meter
    cout_revien_ent_row = cout_revien_df[cout_revien_df['PRODUTS'] == 'CoutRevienENT']
    cout_revien_enm3_row = cout_revien_df[cout_revien_df['PRODUTS'] == 'CoutRevienENM3']
    
    # Store the product names
    GRAPHCOUTREVIEN["PRODUCTSNAME"] = product_columns
    
    for client in clients:
        index = info_clients_df[info_clients_df["NOM DU CLIENT"] == client].index
        if not index.empty:  # Check if client exists in the DataFrame
            client_data = info_clients_df.iloc[index[0]]  # Get first matching row
            clients_products_price = client_data[20:].tolist()  # Get data from index 20 onwards
            
            for i, client_data_price in enumerate(clients_products_price):
                # Skip if the value is not a string or is empty
                if not isinstance(client_data_price, str) or not client_data_price:
                    continue
                
                # Split by "/"
                extractor = client_data_price.split("/")
                if len(extractor) >= 2:
                    price_value = extractor[0]
                    unit_type = extractor[1]
                    
                    product_name = product_columns[i]
                    
                    # Add to GRAPHCOUTREVIEN
                    GRAPHCOUTREVIEN["PRIXVENTE"].append(float(price_value))
                    GRAPHCOUTREVIEN["UNITE"].append(unit_type)
                    
                    # Determine the correct cost based on unit type
                    if unit_type == "T":
                        # Get cost from CoutRevienENT row for this product
                        cost = cout_revien_ent_row[product_name].values[0]
                    elif unit_type == "M3":
                        # Get cost from CoutRevienENM3 row for this product
                        cost = cout_revien_enm3_row[product_name].values[0]
                    else:
                        cost = None
                    
                    GRAPHCOUTREVIEN["COUTREVIEN"].append(float(cost))
                    
                    # Update ScopeUNITE to keep track of units for each product
                    ScopeUNITE[product_name] = unit_type

    
    RESULT  = {"PRODUCTSNAME" : GRAPHCOUTREVIEN["PRODUCTSNAME"],"MARGE" : calculate_marge(GRAPHCOUTREVIEN["PRIXVENTE"], GRAPHCOUTREVIEN["COUTREVIEN"]) }

    
    return RESULT
