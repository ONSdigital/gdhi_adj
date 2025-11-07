"""Module for pivoting adjustment data in the gdhi_adj project."""

import pandas as pd


def pivot_adjustment_long(df: pd.DataFrame) -> pd.DataFrame:
    """
    Un-pivot (melt) the adjustment DataFrame from wide to long format.

    Args:
        df (pd.DataFrame): DataFrame containing data to be adjusted.

    Returns:
        pd.DataFrame: Pivoted DataFrame in long format.
    """
    # Create lists of GDHI columns
    uncon_cols = [col for col in df.columns if col[0].isdigit()]
    con_cols = [col for col in df.columns if col.startswith("CON_")]

    df.rename(columns={"year": "year_to_adjust"}, inplace=True)

    df_uncon = df.melt(
        id_vars=[
            "lsoa_code",
            "lsoa_name",
            "lad_code",
            "lad_name",
            "adjust",
            "year_to_adjust",
        ],
        value_vars=uncon_cols,
        var_name="year",
        value_name="uncon_gdhi",
    )

    df_con = df.melt(
        id_vars=[
            "lsoa_code",
            "lsoa_name",
            "lad_code",
            "lad_name",
            "adjust",
            "year_to_adjust",
        ],
        value_vars=con_cols,
        var_name="year",
        value_name="con_gdhi",
    ).reset_index(drop=True)
    df_con["year"] = df_con["year"].str.replace("^CON_", "", regex=True)

    df_combined = df_uncon.merge(
        df_con,
        on=[
            "lsoa_code",
            "lsoa_name",
            "lad_code",
            "lad_name",
            "adjust",
            "year_to_adjust",
            "year",
        ],
        how="left",
    )
    df_combined["year"] = df_combined["year"].astype(int)

    return df_combined


def pivot_wide_final_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots the DataFrame from long to wide format.

    Args:
        df (pd.DataFrame): The input DataFrame in long format.

    Returns:
        pd.DataFrame: The pivoted DataFrame in wide format.
    """
    df = df.drop(columns=["uncon_gdhi"])

    # Pivot wide to get dates as columns
    df_wide = df.pivot(
        index=["lsoa_code", "lsoa_name", "lad_code", "lad_name"],
        columns="year",
        values="con_gdhi",
    )
    df_wide.columns.name = None  # This removes the label from columns
    df_wide = df_wide.reset_index()

    return df_wide
