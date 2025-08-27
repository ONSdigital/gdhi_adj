"""Module for pivoting data in the gdhi_adj project."""

import pandas as pd


def pivot_years_long_dataframe(
    df: pd.DataFrame, new_var_col: str, new_val_col: str
) -> pd.DataFrame:
    """
    Pivots the DataFrame based on specified index, columns, and values.

    Args:
        df (pd.DataFrame): The input DataFrame.
        new_var_col (str): The name for the column containing old column names.
        new_val_col (str): The name for the column containing values.

    Returns:
    pd.DataFrame: The pivoted DataFrame.
    """
    id_cols = [col for col in df.columns if col[0].isalpha()]
    df = df.melt(id_vars=id_cols, var_name=new_var_col, value_name=new_val_col)

    # convert year column dtype from str to int
    df["year"] = df["year"].astype(int)

    return df


def pivot_output_long(
    df: pd.DataFrame, annual_gdhi: str, con_gdhi: str
) -> pd.DataFrame:
    """Pivots the output DataFrame to long format.
    Args:
        df (pd.DataFrame): The input DataFrame in wide format.
        annual_gdhi (str): The column name for annual GDHI.
        con_gdhi (str): The column name for constrained GDHI.

    Returns:
        pd.DataFrame: The pivoted DataFrame in long format.
    """
    df.rename(columns={annual_gdhi: "annual"}, inplace=True)
    df.rename(columns={con_gdhi: "CONLSOA"}, inplace=True)

    # Pivot long to get a single 'metric' column with names as values
    df = df.melt(
        id_vars=[
            "lsoa_code",
            "lsoa_name",
            "lad_code",
            "lad_name",
            "year",
            "master_z_flag",
            "master_iqr_flag",
            "master_flag",
        ],
        value_vars=["annual", "CONLSOA"],
        var_name="metric",
        value_name="value",
    )
    df["metric_date"] = df["metric"] + "_" + df["year"].astype(str)

    return df


def pivot_wide_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots the DataFrame from long to wide format.

    Args:
        df (pd.DataFrame): The input DataFrame in long format.

    Returns:
        pd.DataFrame: The pivoted DataFrame in wide format.
    """
    # Pivot wide to get dates as columns
    df = df.pivot(
        index=[
            "lsoa_code",
            "lsoa_name",
            "lad_code",
            "lad_name",
            "master_z_flag",
            "master_iqr_flag",
            "master_flag",
        ],
        columns="metric_date",
        values="value",
    )

    df.columns.name = None  # This removes the 'metric_date' label from columns
    df = df.reset_index()

    df.rename(
        columns=lambda col: (
            col.replace("annual_", "") if "annual_" in col else col
        ),
        inplace=True,
    )

    # Reorder columns: move those with 'conlsoa' to the end
    cols = df.columns.tolist()

    reorder_cols = [col for col in cols if "flag" in col or "CONLSOA" in col]
    other_cols = [col for col in cols if col not in reorder_cols]

    df = df[other_cols + reorder_cols]

    return df
