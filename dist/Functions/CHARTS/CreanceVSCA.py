import pandas as pd
import numpy as np

import pandas as pd
import numpy as np

def CreanceVsCA(sells_df, creance_client_df, clientName, fin_date):
    """
    Provide detailed monthly financial analysis for specified clients.
    
    Parameters:
    - sells_df: DataFrame containing sales information
    - creance_client_df: DataFrame containing client receivables
    - clientName: List of client names to filter
    - fin_date: End date for filtering receivables
    
    Returns:
    - DataFrame with monthly financial details
    """
    # Validate input parameters
    if not isinstance(clientName, list):
        raise ValueError("clientName must be a list of client names")
    
    if not isinstance(sells_df, pd.DataFrame) or not isinstance(creance_client_df, pd.DataFrame):
        raise ValueError("sells_df and creance_client_df must be pandas DataFrames")
    
    # Ensure Date column is in datetime format
    creance_client_df['Date'] = pd.to_datetime(creance_client_df['Date'])
    sells_df['Date'] = pd.to_datetime(sells_df['Date'])
    
    # Filter creance_client_df for specified clients and date
    creance_client_dataframe = creance_client_df[
        (creance_client_df["Client"].isin(clientName)) & 
        (creance_client_df["Date"] <= pd.to_datetime(fin_date))
    ]
    
    # Filter for REGLEMENT and IMPAYE operations
    reglements_df = creance_client_dataframe[
        creance_client_dataframe["Type d'opération"] == "REGLEMENT"
    ].copy()
    
    impayes_df = creance_client_dataframe[
        creance_client_dataframe["Type d'opération"] == "IMPAYE"
    ].copy()
    
    # Filter sells data for the same period and clients
    sells_filtered = sells_df[
        (sells_df["Client"].isin(clientName)) & 
        (sells_df["Date"] <= pd.to_datetime(fin_date))
    ]
    
    # Group sells data by month and calculate gross turnover
    sells_monthly = sells_filtered.groupby(
        [sells_filtered['Date'].dt.to_period('M')]
    )['CA BRUT'].sum().reset_index()
    
    # Group receivables by month and calculate net receivables
    receivables_monthly = creance_client_dataframe.groupby(
        [creance_client_dataframe['Date'].dt.to_period('M')]
    ).agg({
        'Solde Débit': 'sum',
        'Solde Crédit': 'sum'
    }).reset_index()
    
    # Group payments by month
    reglements_monthly = reglements_df.groupby(
        [reglements_df['Date'].dt.to_period('M')]
    )['Solde Crédit'].sum().reset_index()
    
    # Group unpaid amounts by month
    impayes_monthly = impayes_df.groupby(
        [impayes_df['Date'].dt.to_period('M')]
    )['Solde Débit'].sum().reset_index()
    
    # Initialize columns for tracking balances
    receivables_monthly['Previous Balance'] = 0
    receivables_monthly['Net Receivables'] = 0
    
    # Calculate running balance month by month
    for i in range(len(receivables_monthly)):
        if i == 0:
            # First month: Net = Debit - Credit
            receivables_monthly.loc[i, 'Net Receivables'] = (
                receivables_monthly.loc[i, 'Solde Débit'] - 
                receivables_monthly.loc[i, 'Solde Crédit']
            )
        else:
            # Get previous month's ending balance
            prev_balance = receivables_monthly.loc[i-1, 'Net Receivables']
            receivables_monthly.loc[i, 'Previous Balance'] = prev_balance
            
            # Current month: Net = Previous Balance + Current Debit - Current Credit
            receivables_monthly.loc[i, 'Net Receivables'] = (
                prev_balance +
                receivables_monthly.loc[i, 'Solde Débit'] - 
                receivables_monthly.loc[i, 'Solde Crédit']
            )
    
    print("Monthly Receivables Analysis:")
    print(receivables_monthly)
    
    # Merge sales and receivables data
    monthly_analysis = pd.merge(
        sells_monthly, 
        receivables_monthly[['Date', 'Net Receivables']], 
        on='Date', 
        how='outer'
    ).fillna(0)
    
    # Merge payments data with monthly analysis
    monthly_analysis = pd.merge(
        monthly_analysis,
        reglements_monthly[['Date', 'Solde Crédit']].rename(columns={'Solde Crédit': 'Reglements'}),
        on='Date',
        how='outer'
    ).fillna(0)
    
    # Merge unpaid data with monthly analysis
    monthly_analysis = pd.merge(
        monthly_analysis,
        impayes_monthly[['Date', 'Solde Débit']].rename(columns={'Solde Débit': 'Impayes'}),
        on='Date',
        how='outer'
    ).fillna(0)
    
    # Add additional columns for easier analysis
    monthly_analysis['Month'] = monthly_analysis['Date'].dt.strftime('%B')
    monthly_analysis['Year'] = monthly_analysis['Date'].dt.strftime('%Y')
    
    # Sort by date and reset index
    monthly_analysis = monthly_analysis.sort_values('Date').reset_index(drop=True)
    
    # Convert the results to lists
    dates_list = monthly_analysis['Date'].dt.strftime('%Y-%m').tolist()
    ca_brut_list = monthly_analysis['CA BRUT'].tolist()
    net_receivables_list = monthly_analysis['Net Receivables'].tolist()
    reglements_list = monthly_analysis['Reglements'].tolist()
    impayes_list = monthly_analysis['Impayes'].tolist()
    
    # Return all lists in the dictionary
    return {
        'dates': dates_list,
        'ca_brut': ca_brut_list,
        'net_receivables': net_receivables_list,
        'reglements': reglements_list,
        'impayes': impayes_list
    }

# Optional: Function to display the results in a more readable format
def display_monthly_analysis(analysis_results):
    """
    Display the monthly financial analysis in a formatted manner.
    """
    print("Monthly Financial Analysis:")
    df = analysis_results['dataframe']
    for _, row in df.iterrows():
        print(f"{row['Month']} {row['Year']}:")
        print(f"  Gross Turnover: {row['CA BRUT']:.2f}")
        print(f"  Net Receivables: {row['Net Receivables']:.2f}")
        print()