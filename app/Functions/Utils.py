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

    return [
        (num / denom if denom != 0 else default_value)
        for num, denom in zip(list1, list2)
    ]


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
