def RepartitionModesPayments(info_clients_df, Creance_df, Creance_Total):
    # Get unique payment modes
    Client_data = info_clients_df["Mode de Paiement"].unique()
    Mode_Payement = []

    # Process each unique payment mode
    for mode in Client_data:
        try:
            if isinstance(mode, str):
                # Split if contains delimiter and strip whitespace
                if "-" in mode:
                    modes = [m.strip() for m in mode.split("-")]
                    Mode_Payement.extend(modes)
                else:
                    Mode_Payement.append(mode.strip())
        except Exception as e:
            print(f"Error processing mode {mode}: {str(e)}")
            continue

    # Remove duplicates and None values
    Mode_Payement = [mode for mode in Mode_Payement if mode]
    Mode_Payement = list(dict.fromkeys(
        Mode_Payement))  # preserve order while removing duplicates

    # Filter creance data
    filtered_creance = Creance_df[
        (Creance_df["Type d'opération"] != "REMPLACEMENT REG")
        & (Creance_df["Type d'opération"] != "IMPAYE")]
    filtered_Creance_Total = Creance_Total[
        (Creance_Total["Type d'opération"] != "REMPLACEMENT REG")
        & (Creance_Total["Type d'opération"] != "IMPAYE")]
    # Calculate payments for each mode
    payments = []
    total_payments = []
    for mode in Mode_Payement:
        try:
            mode_sum = filtered_creance[filtered_creance["Valeur"].str.strip()
                                        == mode]["Solde Crédit"].sum()
            payments.append(mode_sum)

        except Exception as e:
            print(f"Error calculating sum for mode {mode}: {str(e)}")
            payments.append(
                0)  # or handle error differently based on requirements
    for Second_mode in Mode_Payement:
        try:
            Second_mode_sum = filtered_Creance_Total[
                filtered_Creance_Total["Valeur"].str.strip() ==
                Second_mode]["Solde Crédit"].sum()
            total_payments.append(Second_mode_sum)
        except Exception as e:
            print(f"Error calculating sum for mode {Second_mode}: {str(e)}")
            total_payments.append(0)
    # devide the payment and total payement and output percentange in list
    percentage = [
        round(payment / total_payment * 100, 2)
        for payment, total_payment in zip(payments, total_payments)
    ]

    return {
        'Mode_Payement': Mode_Payement,
        'payments': payments,
        'total_PF_payments': total_payments,
        'percentage': percentage,
    
    }