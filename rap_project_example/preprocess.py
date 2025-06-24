def remove_nan_rows(df):
    """
    Removes rows where the 'area' or 'id' columns have NaN values.

    Parameters:
    df (pd.DataFrame): The input DataFrame.

    Returns:
    pd.DataFrame: The DataFrame with NaN rows removed.
    """
    return df.dropna(subset=['Area', 'ID'])