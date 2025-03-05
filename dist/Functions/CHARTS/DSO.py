import pandas as pd
from datetime import datetime


def calculate_client_DSO(data_frame_filtred, client_delay_days):
    # Ensure 'Date' is in datetime format
    data_frame_filtred["Date"] = pd.to_datetime(data_frame_filtred["Date"])
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
    
    # Create a dictionary for each client's data
    client_data = {}
    client_names = []  # New list for client names
    
    for client in clients:
        client_df = monthly_dso[monthly_dso['Client'] == client]
        client_names.append(client)  # Add client name to the list
        
        # Handle client_delay_days - take first element and convert to int
        delay_days = client_delay_days[client] if isinstance(client_delay_days, dict) else client_delay_days
        if isinstance(delay_days, list):
            delay_days = int(delay_days[0])
        
        client_data[client] = {
            'dates': client_df['Month'].dt.strftime('%m-%y').tolist(),
            'max_dso': client_df['Max DSO'].tolist(),
            'avg_dso': client_df['Average DSO'].tolist(),
            'total_credit': client_df['Total Credit Amount'].tolist(),
            'client_delay_days': delay_days
        }

    # Add client_names list to the return dictionary
    return {
        'client_names': client_names,
        'data': client_data
    }
