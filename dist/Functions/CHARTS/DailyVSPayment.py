import pandas as pd
from datetime import datetime


def calculate_dialy_vs_payment_date(data_frame_filtred, client_delay_days):
    # Print the problematic value to see what's causing the issue
    

    # Use errors='coerce' to handle invalid dates
    data_frame_filtred["Date"] = pd.to_datetime(data_frame_filtred["Date"], errors='coerce')

    # Identify which rows have invalid dates
    invalid_date_mask = data_frame_filtred["Date"].isna()
    if invalid_date_mask.any():
        print("Rows with invalid dates:", data_frame_filtred[invalid_date_mask])

    # Remove rows with invalid dates (optional)
    data_frame_filtred = data_frame_filtred.dropna(subset=["Date"])

    # Ensure 'Date' is in datetime format
    data_frame_filtred["Date D'éachéance"] = pd.to_datetime(data_frame_filtred["Date D'éachéance"])

    # Filter rows where 'Solde Crédit' is greater than 0
    filtered_data = data_frame_filtred[
        (data_frame_filtred["Type d'opération"] == "REGLEMENT") & 
        (data_frame_filtred["Solde Crédit"] > 0)
    ]

    # Calculate DSO as days between invoice date and payment date
    filtered_data['DSO'] = (filtered_data["Date D'éachéance"] - filtered_data["Date"]).dt.days

    # Group by client and month
    monthly_dso = filtered_data.groupby(
        [filtered_data["Client"], filtered_data["Date"].dt.to_period("M")]
    ).agg({
        'DSO': ['max', 'mean'],
        'Solde Crédit': 'sum'
    }).reset_index()

    # Flatten the multi-level columns
    monthly_dso.columns = ['Client', 'Month', 'Max DSO', 'Average DSO', 'Total Credit Amount']

    # Convert Period to datetime for easier reading
    monthly_dso['Month'] = monthly_dso['Month'].dt.to_timestamp()

    # Get unique clients
    clients = monthly_dso['Client'].unique().tolist()
    
    # Create the return structure
    result = {
        "DSO_CLIENTS_CHART": {
            "client_names": clients,
            "data": {}
        }
    }
    
    # Fill in data for each client
    for client in clients:
        client_df = monthly_dso[monthly_dso['Client'] == client]
        
        # Handle client_delay_days
        delay_days = client_delay_days[client] if isinstance(client_delay_days, dict) else client_delay_days
        if isinstance(delay_days, list):
            delay_days = int(delay_days[0])
        
        result["DSO_CLIENTS_CHART"]["data"][client] = {
            'dates': client_df['Month'].dt.strftime('%m-%y').tolist(),
            'max_dso': client_df['Max DSO'].tolist(),
            'avg_dso': client_df['Average DSO'].tolist(),
            'total_credit': client_df['Total Credit Amount'].tolist(),
            'client_delay_days': delay_days
        }

    return result
