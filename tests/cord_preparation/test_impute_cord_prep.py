import pandas as pd

from gdhi_adj.cord_preparation.impute_cord_prep import impute_suppression_x


def test_impute_suppression_x():
    """Test the impute_suppression_x function returns the expected midpoint row
    """
    df = pd.DataFrame({
        "lad_code": ["E1", "E1", "S2", "S2", "95A", "95A",],
        "transaction": ["D33", "D623", "D33", "D623", "D33", "D623",],
        "2010": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0,],
        "2011": [20.0, 21.0, 22.0, 23.0, 24.0, 25.0,],
        "2012": [30.0, 31.0, 32.0, 33.0, 34.0, 35.0,],
        "2013": [40.0, 41.0, 42.0, 43.0, 44.0, 45.0,],
    })

    target_cols = ["2010", "2011", "2012",]
    transaction_col = "transaction"
    lad_col = "lad_code"
    transaction_value = "D623"
    lad_val = ["95", "S"]

    result_df = impute_suppression_x(
        df, target_cols=target_cols, transaction_col=transaction_col,
        lad_col=lad_col, transaction_value=transaction_value, lad_val=lad_val
    )

    expected_df = pd.DataFrame({
        "lad_code": ["E1", "E1", "S2", "S2", "95A", "95A",],
        "transaction": ["D33", "D623", "D33", "D623", "D33", "D623",],
        "2010": ["10.0", "11.0", "12.0", "X", "14.0", "X",],
        "2011": ["20.0", "21.0", "22.0", "X", "24.0", "X",],
        "2012": ["30.0", "31.0", "32.0", "X", "34.0", "X",],
        "2013": [40.0, 41.0, 42.0, 43.0, 44.0, 45.0,],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
