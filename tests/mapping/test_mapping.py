"""Unit tests for mapping.py functions using pytest."""

# import pandas as pd
# import pytest

# from gdhi_adj.mapping.mapping_main import (
#     aggregate_lad,
#     cleam_validate_mapper,
#     join_mapper,
#     reformat,
#     rename_s30_to_lad,
# )

# @pytest.mark.parametrize(
#     "df_input,expected_columns,expected_mapping",
#     [
#         (
#             pd.DataFrame({
#                 "lad_code": ["S301", "S302"],
#                 "lad_name": ["Area1", "Area2"]
#             }),
#             ["data_lau_code", "data_lau_name"],
#             True
#         ),
#         (
#             pd.DataFrame({
#                 "lad_code": ["E101", "E102"],
#                 "lad_name": ["Area1", "Area2"]
#             }),
#             ["lad_code", "lad_name"],
#             False
#         ),
#         (
#             pd.DataFrame({
#                 "other_col": ["X", "Y"]
#             }),
#             ["other_col"],
#             False
#         )
#     ]
# )
# def test_rename_s30_to_lad(df_input, expected_columns, expected_mapping):
#     """Test renaming S30 codes to LAU columns."""
#     config = {
#         "mapping": {
#             "data_lad_code": "lad_code",
#             "data_lad_name": "lad_name"
#         }
#     }
#     df_result, mapping_flag = rename_s30_to_lad(config, df_input.copy())
#     assert set(expected_columns).issubset(df_result.columns)
#     assert mapping_flag == expected_mapping
#     assert True


# def test_cleam_validate_mapper():
#     """Test mapper cleaning and validation."""
#     df = pd.DataFrame({
#         "mapper_lad_code": ["L1", "L2", "L1"],
#         "mapper_lad_name": ["Name1", "Name2", "Name1"],
#         "mapper_lau_code": ["S1", "S2", "S1"],
#         "mapper_lau_name": ["LAU1", "LAU2", "LAU1"],
#         "extra_col": [1, 2, 1]
#     })
#     cleaned_df, valid = cleam_validate_mapper(df)
#     assert valid is True
#     assert "extra_col" not in cleaned_df.columns
#     assert len(cleaned_df) == 2


# def test_join_mapper():
#     """Test joining mapper to input dataframe."""
#     df = pd.DataFrame({
#         "data_lau_code": ["S1", "S2"],
#         "data_lau_name": ["LAU1", "LAU2"]
#     })
#     mapper_df = pd.DataFrame({
#         "mapper_lau_code": ["S1", "S2"],
#         "mapper_lad_code": ["L1", "L2"],
#         "mapper_lad_name": ["Name1", "Name2"]
#     })
#     result_df = join_mapper(df, mapper_df)
#     assert "mapper_lad_code" in result_df.columns
#     assert "data_lau_code" not in result_df.columns


# def test_aggregate_lad():
#     """Test LAD-level aggregation."""
#     df = pd.DataFrame({
#         "mapper_lad_code": ["L1", "L1", "L2"],
#         "mapper_lad_name": ["Name1", "Name1", "Name2"],
#         "2020": [1, 2, 3],
#         "2021": [4, 5, 6],
#         "extra": ["A", "A", "B"]
#     })
#     agg_df = aggregate_lad(df)
#     assert len(agg_df) == 2
#     assert agg_df.loc[
#         agg_df["mapper_lad_code"] == "L1", "2020"
#     ].values[0] == 3


# def test_reformat():
#     """Test reformatting final dataframe."""
#     df = pd.DataFrame({
#         "mapper_lad_code": ["L1"],
#         "mapper_lad_name": ["Name1"],
#         "2020": [1]
#     })
#     original_columns = ["lad_code", "lad_name", "2020"]
#     reformatted_df = reformat(df, original_columns)
#     assert list(reformatted_df.columns) == original_columns
#     assert reformatted_df["lad_code"].iloc[0] == "L1"
