
def output_to_csv(dataframe, file_path):
    """
    Outputs the given DataFrame to a CSV file.

    Parameters:
    dataframe (pd.DataFrame): The DataFrame to output.
    file_path (str): The path to the CSV file.
    """
    dataframe.to_csv(file_path, index=False)
