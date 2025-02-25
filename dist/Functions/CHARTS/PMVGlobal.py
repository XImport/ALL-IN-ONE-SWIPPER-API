from ..Utilitys.Utils import aggregate_time_series, List_Division

def prepare_pmv_data(filtered_data, group_by_month):
    """Prepare PMV-related chart data."""
    
    def aggregate_by_type(df, type_name):
        type_data = df[df["Type"] == type_name]
     
        # Check if any data exists for this type
        if type_data.empty:
            # If using dates from the complete dataset, return zeros matching that length
            return [0] * len(all_dates)
        
        aggregated = aggregate_time_series(
            type_data, "Date", ["CA Net", "Qté en T"], group_by_month
        )
        
        # Get the dates for this type
        type_dates = aggregated["Date"].tolist()
        
        # Perform element-wise division using List_Division
        ca_net_list = aggregated["CA Net"].tolist()
        qty_list = aggregated["Qté en T"].tolist()
        result = List_Division(ca_net_list, qty_list)
        
        # Create a mapping of date to result
        date_to_result = {date: value for date, value in zip(type_dates, result)}
        
        # Create final list aligned with all_dates
        final_result = []
        for date in all_dates:
            if date in date_to_result:
                final_result.append(float(date_to_result[date]))
            else:
                final_result.append(0)
                
        return final_result
    
    # Aggregate dates from the complete dataset
    dates = aggregate_time_series(
        filtered_data, "Date", ["CA Net", "Qté en T"], group_by_month
    )
    
    # Get the list of all dates
    all_dates = dates["Date"].tolist()
    
    # Prepare data for each type
    nobles_data = aggregate_by_type(filtered_data, "Nobles")
    graves_data = aggregate_by_type(filtered_data, "Graves")
    sterile_data = aggregate_by_type(filtered_data, "Stérile")
    
    return {
        "PMVDATES": all_dates,
        "PMVNOBLES": nobles_data,
        "PMVGRAVES": graves_data,
        "PMVSTERILE": sterile_data,
    }