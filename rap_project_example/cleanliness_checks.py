import pandas as pd


def check_column_names(df: pd.DataFrame) -> None:
    required_columns = {'ID', 'Area', 'Proportion', 'Type'}
    if set(df.columns) == required_columns:
        print("Column names correct")
    else:
        print("Column names incorrect")


def check_missing_data(df: pd.DataFrame) -> None:
    missing_data = df[df.isnull().any(axis=1)]
    if not missing_data.empty:
        print("Rows with missing data:")
        print(missing_data)
    else:
        print("No missing data found")
