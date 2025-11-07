import pandas as pd
import pytest

from gdhi_adj.preprocess.join_preprocess import (
    concat_wide_dataframes,
    constrain_to_reg_acc,
)


class TestConstrainToRegAcc:
    """Test suite for the constrain_to_reg_acc function."""

    def test_constrain_to_reg_acc_num_input(self):
        """Test the constrain_to_reg_acc function."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E02", "E01", "E01", "E02"],
            "year": [2001, 2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": [10, 20, 30, 45, 50, 70],
            "mean_non_out_gdhi": [15, 15, 25, 45, 45, 50],
            "master_flag": [True, True, False, False, False, True],
        })

        reg_acc = pd.DataFrame({
            "Region": ["NE", "NE", "NE", "NE", "NE"],
            "lad_code": ["E01", "E02", "E01", "E02", "E02"],
            "Region name": ["Hart", "Stock", "Hart", "Stock", "Stock"],
            "Transaction code": ["B.2g", "B.2g", "B.2g", "B.3g", "B.2g"],
            "transaction_name": ["Operating surplus", "Operating surplus",
                                 "Operating surplus", "Mixed income",
                                 "Operating surplus"],
            "year": [2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": [1000, 200, 300, 3500, 400]
        })

        transaction_name = "Operating surplus"

        result_df = constrain_to_reg_acc(df, reg_acc, transaction_name)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E02", "E01", "E01", "E02"],
            "year": [2001, 2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": [10, 20, 30, 45, 50, 70],
            "mean_non_out_gdhi": [15, 15, 25, 45, 45, 50],
            "master_flag": ["TRUE", "TRUE", "MEAN", "MEAN", "MEAN", "TRUE"],
            "conlsoa_gdhi": [400.0, 571.429, 109.091, 150.0, 157.895, 233.333],
            "conlsoa_mean": [600.0, 428.571, 90.909, 150.0, 142.105, 166.667]
        })

        pd.testing.assert_frame_equal(result_df, expected_df, rtol=1e-3)

    def test_constrain_to_reg_acc_string_input(self):
        """Test the constrain_to_reg_acc function."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E02", "E01", "E01", "E02"],
            "year": [2001, 2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": [10, 20, 30, 45, 50, 70],
            "mean_non_out_gdhi": [15, 15, 25, 45, 45, 50],
            "master_flag": [True, True, False, False, False, True],
        })

        reg_acc = pd.DataFrame({
            "Region": ["NE", "NE", "NE", "NE", "NE"],
            "lad_code": ["E01", "E02", "E01", "E02", "E02"],
            "Region name": ["Hart", "Stock", "Hart", "Stock", "Stock"],
            "Transaction code": ["B.2g", "B.2g", "B.2g", "B.3g", "B.2g"],
            "transaction_name": ["Operating surplus", "Operating surplus",
                                 "Operating surplus", "Mixed income",
                                 "Operating surplus"],
            "year": [2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": ["1,000", "200", "300", "3,500", "400"]
        })

        transaction_name = "Operating surplus"

        result_df = constrain_to_reg_acc(df, reg_acc, transaction_name)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E02", "E01", "E01", "E02"],
            "year": [2001, 2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": [10, 20, 30, 45, 50, 70],
            "mean_non_out_gdhi": [15, 15, 25, 45, 45, 50],
            "master_flag": ["TRUE", "TRUE", "MEAN", "MEAN", "MEAN", "TRUE"],
            "conlsoa_gdhi": [400.0, 571.429, 109.091, 150.0, 157.895, 233.333],
            "conlsoa_mean": [600.0, 428.571, 90.909, 150.0, 142.105, 166.667]
        })

        pd.testing.assert_frame_equal(result_df, expected_df, rtol=1e-3)

    def test_constrain_to_reg_acc_zero(self):
        """Test the constrain_to_reg_acc function."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E02", "E01", "E01", "E02"],
            "year": [2001, 2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": [10, 0, 30, 45, 50, 70],
            "mean_non_out_gdhi": [15, 0, 25, 45, 45, 50],
            "master_flag": [True, True, False, False, False, True],
        })

        reg_acc = pd.DataFrame({
            "Region": ["NE", "NE", "NE", "NE", "NE"],
            "lad_code": ["E01", "E02", "E01", "E02", "E02"],
            "Region name": ["Hart", "Stock", "Hart", "Stock", "Stock"],
            "Transaction code": ["B.2g", "B.2g", "B.2g", "B.3g", "B.2g"],
            "transaction_name": ["Operating surplus", "Operating surplus",
                                 "Operating surplus", "Mixed income",
                                 "Operating surplus"],
            "year": [2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": ["1,000", "200", "300", "3,500", "400"]
        })

        transaction_name = "Operating surplus"

        result_df = constrain_to_reg_acc(df, reg_acc, transaction_name)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E02", "E01", "E01", "E02"],
            "year": [2001, 2001, 2001, 2002, 2002, 2002],
            "uncon_gdhi": [10, 0, 30, 45, 50, 70],
            "mean_non_out_gdhi": [15, 0, 25, 45, 45, 50],
            "master_flag": ["TRUE", "TRUE", "MEAN", "MEAN", "MEAN", "TRUE"],
            "conlsoa_gdhi": [400.0, 0.0, 109.091, 150.0, 157.895, 233.333],
            "conlsoa_mean": [600.0, 0.0, 90.909, 150.0, 142.105, 166.667]
        })

        pd.testing.assert_frame_equal(result_df, expected_df, rtol=1e-3)

    def test_constrain_to_reg_acc_col_mismatch(self):
        """Test the constrain_to_reg_acc function with column mismatch."""
        df = pd.DataFrame({
            "lsoa_code": [],
            "lad_code": [],
            "year": [],
            "uncon_gdhi": [],
        })

        # Define regional accounts with different column names
        reg_acc = pd.DataFrame({
            "lad_code": [],
            "year": [],
            "Region": [],
            "Region name": [],
            "Transaction code": [],
            "transaction_name": [],
            "wrong_col": []
        })

        transaction_name = "Operating surplus"

        # Ensure error is raised if regional accs columns aren't present in df
        with pytest.raises(
            expected_exception=ValueError,
            match="DataFrames have different columns"
        ):
            constrain_to_reg_acc(df, reg_acc, transaction_name)


def test_concat_wide_dataframes():
    """Test the concat_wide_dataframes function."""
    df_outlier = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "2002": [10.0, 11.0],
        "master_flag": ["TRUE", "TRUE"],
        "CONLSOA_2002": [100.0, 110.0]
    })

    df_mean = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "2002": [20.0, 21.0],
        "master_flag": ["MEAN", "MEAN"],
        "CONLSOA_2002": [200.0, 210.0]
    })

    result_df = concat_wide_dataframes(df_outlier, df_mean)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E2"],
        "2002": [10.0, 20.0, 11.0, 21.0],
        "master_flag": ["TRUE", "MEAN", "TRUE", "MEAN"],
        "CONLSOA_2002": [100.0, 200.0, 110.0, 210.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)
