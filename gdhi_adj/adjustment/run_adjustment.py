"""Module for adjusting data in the gdhi_adj project."""

import os

import pandas as pd

from gdhi_adj.adjustment.apportion_adjustment import (
    apportion_adjustment,
    apportion_negative_adjustment,
    apportion_rollback_years,
    calc_non_outlier_proportions,
)
from gdhi_adj.adjustment.calc_adjustment import (
    extrapolate_imputed_val,
    interpolate_imputed_val,
)
from gdhi_adj.adjustment.filter_adjustment import (  # filter_component,
    filter_adjust,
    filter_year,
)
from gdhi_adj.adjustment.flag_adjustment import identify_safe_years
from gdhi_adj.adjustment.join_adjustment import (
    join_analyst_constrained_data,
    join_analyst_unconstrained_data,
)
from gdhi_adj.adjustment.pivot_adjustment import (
    pivot_adjustment_long,
    pivot_wide_final_dataframe,
)
from gdhi_adj.adjustment.reformat_adjustment import (
    reformat_adjust_col,
    reformat_year_col,
)
from gdhi_adj.preprocess.calc_preprocess import calc_rate_of_change
from gdhi_adj.preprocess.flag_preprocess import flag_rollback_years
from gdhi_adj.utils.helpers import read_with_schema, write_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def run_adjustment(config: dict) -> None:
    """
    Run the adjustment steps for the GDHI adjustment project.

    This function performs the following steps:
    1. Load the configuration settings.
    2. Load the input data.
    3. Reformat adjust and year columns.
    4. Filter of data for adjustment.
    5. Join analyst output with constrained and unconstrained data.
    6. Pivot the DataFrame to long format for manipulation.
    7. Filter data by the specified year range.
    8. Calculate the imputed gdhi values for outlier years.
    9. Calculate adjustment values based on imputed gdhi.
    10. Apportion adjustment values to all years.
    11. Save interim data with all calculated values.
    12. Pivot data to wide format for PowerBI QA reiteration.
    13. Pivot final DataFrame to wide format for exporting.
    14. Save the final adjusted data.


    Args:
        config (dict): Configuration dictionary containing user settings and
        pipeline settings.
    Returns:
        None: The function does not return any value. It saves the processed
        DataFrame to a CSV file.
    """
    logger.info("Adjustment started")

    logger.info("Loading configuration settings")
    local_or_shared = config["user_settings"]["local_or_shared"]
    filepath_dict = config[f"adjustment_{local_or_shared}_settings"]
    schema_path = config["pipeline_settings"]["schema_path"]

    input_adj_file_path = (
        "C:/Users/" + os.getlogin() + filepath_dict["input_adj_file_path"]
    )
    input_constrained_file_path = (
        "C:/Users/"
        + os.getlogin()
        + filepath_dict["input_constrained_file_path"]
    )
    input_unconstrained_file_path = (
        "C:/Users/"
        + os.getlogin()
        + filepath_dict["input_unconstrained_file_path"]
    )

    # match = re.search(
    #     r".*GDHI_Disclosure_(.*?)_[^_]+\.csv", input_unconstrained_file_path
    # )

    # if match:
    #     gdhi_suffix = match.group(1) + "_"
    gdhi_suffix = config["user_settings"]["output_data_prefix"] + "_"

    input_adj_schema_path = (
        schema_path + config["pipeline_settings"]["input_adj_schema_name"]
    )
    input_constrained_schema_path = (
        schema_path
        + config["pipeline_settings"]["input_constrained_schema_name"]
    )
    input_unconstrained_schema_path = (
        schema_path
        + config["pipeline_settings"]["input_unconstrained_schema_name"]
    )

    start_year = config["user_settings"]["start_year"]
    end_year = config["user_settings"]["end_year"]

    sas_code_filter = config["user_settings"]["sas_code_filter"]
    cord_code_filter = config["user_settings"]["cord_code_filter"]
    credit_debit_filter = config["user_settings"]["credit_debit_filter"]

    output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    output_schema_path = (
        schema_path
        + config["pipeline_settings"]["output_adjustment_schema_path"]
    )
    interim_filename = gdhi_suffix + filepath_dict.get(
        "interim_filename", None
    )
    new_filename = gdhi_suffix + filepath_dict.get("output_filename", None)

    logger.info("Reading in data with schemas")
    df_powerbi_output = read_with_schema(
        input_adj_file_path, input_adj_schema_path
    )
    df_constrained = read_with_schema(
        input_constrained_file_path, input_constrained_schema_path
    )
    df_unconstrained = read_with_schema(
        input_unconstrained_file_path, input_unconstrained_schema_path
    )

    logger.info("Reformatting adjust and year columns")
    df_powerbi_output = reformat_adjust_col(df_powerbi_output)

    df_powerbi_output = reformat_year_col(
        df_powerbi_output, start_year, end_year
    )

    logger.info("Filtering for data that requires adjustment")
    df_powerbi_output = filter_adjust(df_powerbi_output)
    # df_constrained = filter_component(
    #     df_constrained,
    #     sas_code_filter,
    #     cord_code_filter,
    #     credit_debit_filter
    # )

    logger.info("Joining analyst output and constrained DAP output")
    df = join_analyst_constrained_data(df_constrained, df_powerbi_output)

    logger.info("Joining analyst output and unconstrained DAP output")
    df = join_analyst_unconstrained_data(df_unconstrained, df)

    logger.info("Pivoting DataFrame long")
    df = pivot_adjustment_long(df)

    logger.info("Filtering data for specified years")
    df = filter_year(df, start_year, end_year)

    logger.info(
        "Flagging rollback years from: "
        f"{config["user_settings"]["rollback_year_start"]}:"
        f"{config["user_settings"]["rollback_year_end"]}"
    )
    df = calc_rate_of_change(
        df,
        ascending=False,
        sort_cols=["lsoa_code", "year"],
        group_col="lsoa_code",
        val_col="uncon_gdhi",
    )
    df = calc_rate_of_change(
        df,
        ascending=True,
        sort_cols=["lsoa_code", "year"],
        group_col="lsoa_code",
        val_col="uncon_gdhi",
    )
    df = flag_rollback_years(
        df,
        rollback_year_start=config["user_settings"]["rollback_year_start"],
        rollback_year_end=config["user_settings"]["rollback_year_end"],
    )

    logger.info("Identifying non-outlier years for imputation")
    df, imputed_df = identify_safe_years(df, start_year, end_year)

    logger.info("Calculating values to be imputed")
    imputed_df = interpolate_imputed_val(imputed_df)
    imputed_df = extrapolate_imputed_val(df, imputed_df)

    logger.info("Calculating non-outlier proportions")
    df = calc_non_outlier_proportions(df)

    logger.info("Apportioning adjustment values to all LSOAs")
    df = apportion_adjustment(df, imputed_df)

    if config["user_settings"]["accept_negatives"] is False:
        logger.info("Apportioning negative adjusted values")
        df = apportion_negative_adjustment(df)

    logger.info("Apportion rollback years.")
    df = apportion_rollback_years(df)

    logger.info("Saving interim data")
    qa_df = pd.DataFrame(
        {
            "config": [
                f"sas_code_filter = {sas_code_filter}",
                f"cord_code_filter = {cord_code_filter}",
                f"credit_debit_filter = {credit_debit_filter}",
            ],
        }
    )
    qa_df.to_csv(
        output_dir + gdhi_suffix + "manual_adj_adjustments_config.txt",
        index=False,
        header=False,
    )

    logger.info(f"{output_dir + interim_filename}")
    df.to_csv(
        output_dir + interim_filename,
        index=False,
    )
    logger.info("Data saved successfully")

    if config["user_settings"]["accept_negatives"] is False:
        df = df.drop(columns=["adjusted_con_gdhi"]).rename(
            columns={"readjusted_con_gdhi": "adjusted_con_gdhi"}
        )

    df = df.drop(
        columns=[
            "con_gdhi",
            "imputed_gdhi",
        ]
    ).rename(columns={"adjusted_con_gdhi": "con_gdhi"})

    logger.info("Pivoting final DataFrame wide for exporting")
    df = pivot_wide_final_dataframe(df)

    # Save output file with new filename if specified
    if config["user_settings"]["output_data"]:
        # Write DataFrame to CSV
        write_with_schema(df, output_schema_path, output_dir, new_filename)
