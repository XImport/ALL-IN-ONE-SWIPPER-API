from datetime import datetime
import os
import pandas as pd


def List_Division(list1, list2, default_value=0):
    """
    Perform element-wise division between two lists, handling division by zero.

    Args:
        list1 (list): The numerator list.
        list2 (list): The denominator list.
        default_value (float): The value to use when division by zero occurs.

    Returns:
        list: A list containing the results of the division.
    """
    if len(list1) != len(list2):
        raise ValueError("Both lists must have the same length.")

    result = []
    for num, denom in zip(list1, list2):
       
        if denom != 0:
            value = num / denom
           
            result.append(value)
        else:
            print(0)
            result.append(default_value)
    
    return result

def get_files_in_directory(directory_path):
    """
    List all files in the specified directory recursively.

    Args:
        directory_path (str): Path to the directory to search

    Returns:
        list: List of file paths found in the directory

    Raises:
        None: All exceptions are caught and logged, returns empty list on error
    """
    try:
        # First check if directory exists
        if not os.path.exists(directory_path):
            print(f"Directory does not exist: {directory_path}")
            return []

        # Check if it's actually a directory
        if not os.path.isdir(directory_path):
            print(f"Path is not a directory: {directory_path}")
            return []

        all_files = []

        # Recursively walk through the directory
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    # Verify file is accessible and not a broken symlink
                    if os.path.exists(file_path) and os.access(file_path, os.R_OK):
                        all_files.append(file_path)
                    else:
                        print(f"File not accessible or broken symlink: {file_path}")
                except Exception as e:
                    print(f"Error accessing file {file_path}: {str(e)}")
                    continue

        if not all_files:
            print(f"No files found in directory: {directory_path}")

        return all_files

    except FileNotFoundError:
        print(f"Directory not found: {directory_path}")
        return []

    except PermissionError:
        print(f"Permission denied to access directory: {directory_path}")
        return []

    except Exception as e:
        print(f"Error in get_files_in_directory: {str(e)}")
        import traceback

        print(f"Stacktrace: {traceback.format_exc()}")
        return []


def Metrics_DATA_Filters(filters, group_by_month):

    Metrics_DATA = aggregate_time_series(
        filters,  # Use provided or default data
        "Date",
        [
            "CA BRUT",
            "CA Net",
            "Qté en T",
            "Qté en m3",
            "Type",
            "CA Transport",
            "Coût de transport",
            "Marge sur Transport",
        ],
        group_by_month,
    )
    return Metrics_DATA


def aggregate_time_series(df, date_column, value_columns, group_by_month=False):
    """
    Aggregate time series data either monthly or daily.

    Args:
        df: DataFrame containing the data
        date_column: Name of the date column
        value_columns: List of columns to aggregate
        group_by_month: Boolean indicating whether to group by month
    """
    # Make a copy of the dataframe to avoid modifying the original
    df_copy = df.copy()

    # Convert date column to datetime if not already
    df_copy[date_column] = pd.to_datetime(df_copy[date_column], errors="coerce")

    # Drop rows where date conversion failed (NaT)
    df_copy = df_copy.dropna(subset=[date_column])

    if df_copy.empty:
        return pd.DataFrame(columns=[date_column] + value_columns)

    # Handle case when grouping by day or month
    if not group_by_month:
        df_copy["group_key"] = df_copy[date_column].dt.date  # Group by day
    else:
        df_copy["group_key"] = df_copy[date_column].dt.strftime(
            "%m/%Y"
        )  # Group by month-year

    # Split columns into numeric and non-numeric
    numeric_cols = [
        col
        for col in value_columns
        if col in df_copy.columns and pd.api.types.is_numeric_dtype(df_copy[col])
    ]
    non_numeric_cols = [
        col
        for col in value_columns
        if col in df_copy.columns and not pd.api.types.is_numeric_dtype(df_copy[col])
    ]

    # Create aggregation dictionary
    agg_dict = {col: "sum" for col in numeric_cols}
    agg_dict.update({col: "first" for col in non_numeric_cols})

    # Perform groupby and aggregation
    result = df_copy.groupby("group_key").agg(agg_dict).reset_index()
    result = result.rename(columns={"group_key": date_column})

    # Explicitly format the date column for daily grouping
    if not group_by_month:
        result[date_column] = pd.to_datetime(result[date_column]).dt.strftime(
            "%d %b %Y"
        )  # Format as '02 Jan 2024'

    # Ensure all requested columns are present
    for col in value_columns:
        if col not in result.columns:
            result[col] = None

    # Convert the date column to string to prevent re-interpretation
    result[date_column] = result[date_column].astype(str)

    return result


def should_aggregate_monthly(start_date, end_date):
    """Determine if data should be aggregated monthly based on date range."""
    # Ensure the input dates are of type datetime
    if not isinstance(start_date, pd.Timestamp):
        start_date = pd.to_datetime(start_date)
    if not isinstance(end_date, pd.Timestamp):
        end_date = pd.to_datetime(end_date)

    # If start and end dates are the same, no aggregation by month is needed
    if start_date == end_date:
        return False

    # Calculate the difference in days
    days_difference = (end_date - start_date).days

    print("resuuuuuuuuuuuult : ", (days_difference))
    # If the date range is more than 20 days, aggregate by month
    return days_difference > 20

def process_client_products(clients, info_clients_df, cout_revien_df):
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
    
    return GRAPHCOUTREVIEN


def calculate_marge(prix_vente_list, cout_achat_list):
    """
    Calculate the margin percentage based on selling price and purchase cost.
    :param prix_vente_list: List of selling prices (HT)
    :param cout_achat_list: List of purchase costs (HT)
    :return: List of margin percentages
    """
    if len(prix_vente_list) != len(cout_achat_list):
        raise ValueError("Both lists must have the same length")
    
    marges = []
    for prix_vente, cout_achat in zip(prix_vente_list, cout_achat_list):
        if prix_vente == 0:
            marges.append(0)  # Avoid division by zero
        else:
            marge = ((prix_vente - cout_achat) / prix_vente) * 100
            marges.append(round(marge, 2))  # Rounded to 2 decimal places
    print(marges)
    return marges


# Example usage:
# GRAPHCOUTREVIEN, ScopeUNITE = process_client_products(clients, info_clients_df, cout_revien_df)
# print("GRAPHCOUTREVIEN:", GRAPHCOUTREVIEN)
# print("ScopeUNITE:", ScopeUNITE)












def prepare_recouvrement_data(filtered_recouvrement, group_by_month):
    """
    Prepare recouvrement chart data from the filtered recouvrement dataframe.
    
    Args:
        filtered_recouvrement (pd.DataFrame): Filtered recouvrement data
        group_by_month (bool): Whether to aggregate by month or by day
        
    Returns:
        dict: Recouvrement chart data with dates and payment amounts
    """
    if filtered_recouvrement.empty:
        return {"DATES": [], "MONTANTS": []}

    required_columns = {"Date de Paiement", "Montant Paye"}
    if not required_columns.issubset(filtered_recouvrement.columns):
        
        return {"DATES": [], "MONTANTS": []}

    try:
        # Aggregate data
        aggregated_data = aggregate_time_series(
            filtered_recouvrement, "Date de Paiement", ["Montant Paye"], group_by_month
        )

        if aggregated_data.empty:
            return {"DATES": [], "MONTANTS": []}

        # Extract dates and amounts
        dates = aggregated_data["Date de Paiement"].tolist()
        montants = aggregated_data["Montant Paye"].tolist()

        # Format dates
        formatted_dates = [
            date if isinstance(date, str) else date.strftime('%m/%Y' if group_by_month else '%d/%m/%Y')
            for date in dates
        ]

        return {"DATES": formatted_dates, "MONTANTS": montants}

    except Exception as e:
       
        return {"DATES": [], "MONTANTS": []}