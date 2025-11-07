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
    df: pd.DataFrame, uncon_gdhi: str, con_gdhi: str
) -> pd.DataFrame:
    """Pivots the output DataFrame to long format.
    Args:
        df (pd.DataFrame): The input DataFrame in wide format.
        uncon_gdhi (str): The column name for unconstrained GDHI.
        con_gdhi (str): The column name for constrained GDHI.

    Returns:
        pd.DataFrame: The pivoted DataFrame in long format.
    """
    pivot_df = df.copy()
    pivot_df.rename(columns={uncon_gdhi: "uncon"}, inplace=True)
    pivot_df.rename(columns={con_gdhi: "CONLSOA"}, inplace=True)

    id_cols = [
        "lsoa_code",
        "lsoa_name",
        "lad_code",
        "lad_name",
        "year",
    ] + [col for col in pivot_df.columns if col.startswith("master_")]

    # Pivot long to get a single 'metric' column with names as values
    pivot_df = pivot_df.melt(
        id_vars=id_cols,
        value_vars=["uncon", "CONLSOA"],
        var_name="metric",
        value_name="value",
    )
    pivot_df["metric_date"] = (
        pivot_df["metric"] + "_" + pivot_df["year"].astype(str)
    )

    return pivot_df


def pivot_wide_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots the DataFrame from long to wide format.

    Args:
        df (pd.DataFrame): The input DataFrame in long format.

    Returns:
        pd.DataFrame: The pivoted DataFrame in wide format.
    """
    id_cols = [
        "lsoa_code",
        "lsoa_name",
        "lad_code",
        "lad_name",
    ] + [col for col in df.columns if col.startswith("master_")]

    # Pivot wide to get dates as columns
    df = df.pivot(
        index=id_cols,
        columns="metric_date",
        values="value",
    )

    df.columns.name = None  # This removes the 'metric_date' label from columns
    df = df.reset_index()

    df.rename(
        columns=lambda col: (
            col.replace("uncon_", "") if "uncon_" in col else col
        ),
        inplace=True,
    )

    # Reorder columns: move those with 'conlsoa' to the end
    cols = df.columns.tolist()

    reorder_cols = [col for col in cols if "flag" in col or "CONLSOA" in col]
    other_cols = [col for col in cols if col not in reorder_cols]

    df = df[other_cols + reorder_cols]

    return df
