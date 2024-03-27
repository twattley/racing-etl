import numpy as np
import pandas as pd
from src.transform.transform_data import (
    get_surfaces_from_tf_rating,
    get_tf_rating_values,
    create_pounds,
    get_inplay_high_and_low,
    create_distance_data,
    create_time_data,
    create_headgear_data,
)
from tests.test_helpers import assert_df_data_equal


import pandas as pd
import re


def print_dataframe_for_testing(df):

    print("pd.DataFrame({")

    for col in df.columns:
        value = df[col].iloc[0]
        if re.match(r"\d{4}-\d{2}-\d{2}", str(value)):
            str_test = (
                "[" + " ".join([f"pd.Timestamp('{x}')," for x in list(df[col])]) + "]"
            )
            print(f"'{col}':{str_test},")
        else:
            print(f"'{col}':{list(df[col])},")
    print("})")


# def test_tf_rating_values():
#     input_df = pd.DataFrame(
#         {
#             "tf_tf_rating": [
#                 "",
#                 "+",
#                 "?",
#                 "1",
#                 "1+",
#                 "1?",
#                 "a1",
#                 "a1+",
#                 "a1?",
#                 "p1",
#                 "p1+",
#                 "p1?",
#                 "f1",
#                 "f1+",
#                 "f1?",
#             ],
#         }
#     )

#     output_df = input_df.pipe(get_surfaces_from_tf_rating).pipe(get_tf_rating_values)

#     expected_df = pd.DataFrame(
#         {
#             "tf_tf_rating": [
#                 "",
#                 "+",
#                 "?",
#                 "1",
#                 "1+",
#                 "1?",
#                 "a1",
#                 "a1+",
#                 "a1?",
#                 "p1",
#                 "p1+",
#                 "p1?",
#                 "f1",
#                 "f1+",
#                 "f1?",
#             ],
#             "surface": [
#                 "turf",
#                 "turf",
#                 "turf",
#                 "turf",
#                 "turf",
#                 "turf",
#                 "artificial",
#                 "artificial",
#                 "artificial",
#                 "polytrack",
#                 "polytrack",
#                 "polytrack",
#                 "fibresand",
#                 "fibresand",
#                 "fibresand",
#             ],
#             "tf_rating": [np.nan, np.nan, np.nan, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
#             "tf_rating_view": [
#                 "neutral",
#                 "positive",
#                 "questionable",
#                 "neutral",
#                 "positive",
#                 "questionable",
#                 "neutral",
#                 "positive",
#                 "questionable",
#                 "neutral",
#                 "positive",
#                 "questionable",
#                 "neutral",
#                 "positive",
#                 "questionable",
#             ],
#         }
#     )

#     expected_df["tf_rating"] = expected_df["tf_rating"].astype("Int64")

#     pd.testing.assert_frame_equal(
#         output_df.sort_index(axis=1).reset_index(drop=True),
#         expected_df.sort_index(axis=1).reset_index(drop=True),
#     )


# def test_create_pounds():
#     input_df = pd.DataFrame({"rp_horse_weight": ["8-11", "9-11", "10-11", ""]})
#     output_df = input_df.pipe(create_pounds)

#     expected_df = pd.DataFrame(
#         {
#             "rp_horse_weight": ["8-11", "9-11", "10-11", ""],
#             "weight_carried_st": [8, 9, 10, np.nan],
#             "weight_carried_extra_lbs": [11, 11, 11, np.nan],
#             "weight_carried_lbs": [123, 137, 151, np.nan],
#         }
#     )
#     for i in [
#         "weight_carried_st",
#         "weight_carried_extra_lbs",
#         "weight_carried_lbs",
#     ]:
#         expected_df[i] = expected_df[i].astype("Int64")

#     pd.testing.assert_frame_equal(output_df, expected_df)


def test_get_inplay_high_and_low():
    input_df = pd.DataFrame({"tf_in_play_prices": ["-/2", "2/-", "/-", "/", ""]})
    output_df = input_df.pipe(get_inplay_high_and_low)
    print_dataframe_for_testing(output_df)

    pd.testing.assert_frame_equal(
        output_df,
        pd.DataFrame(
            {
                "tf_in_play_prices": ["-/2", "2/-", "/-", "/", ""],
                "in_play_low": [2.0, np.nan, np.nan, np.nan, np.nan],
                "in_play_high": [np.nan, 2.0, np.nan, np.nan, np.nan],
            }
        ),
    )


# def test_create_distance_data():
#     input_df = pd.DataFrame(
#         {
#             "tf_distance": [
#                 "6f",
#                 "6f 70y",
#                 "1m",
#                 "1m 70y",
#                 "1m 1f",
#                 "1m 1f 70y",
#             ]
#         }
#     )

#     output_df = input_df.pipe(create_distance_data)
#     print_dataframe_for_testing(output_df)

#     expected_df = pd.DataFrame(
#         {
#             "tf_distance": ["6f", "6f 70y", "1m", "1m 70y", "1m 1f", "1m 1f 70y"],
#             "distance_yards": [1320.0, 1390.0, 1760.0, 1830.0, 1980.0, 2050.0],
#             "distance_meters": [1207.01, 1271.02, 1609.34, 1673.35, 1810.51, 1874.52],
#             "distance_kilometers": [1.21, 1.27, 1.61, 1.67, 1.81, 1.87],
#         }
#     )
#     pd.testing.assert_frame_equal(output_df, expected_df)


def test_create_time_data():
    input_df = pd.DataFrame(
        {
            "rp_winning_time": [
                "1m 1.1s",
                "1m 1.1s",
                "1m 1.1s",
                "1m 1.1s",
                "1m 1.1s",
            ]
        }
    )
    output_df = input_df.pipe(create_time_data)
    print_dataframe_for_testing(output_df)

    expected_df = pd.DataFrame(
        {
            "rp_winning_time": [
                "",
                "0.00s",
                "0.00s (standard time)",
                "10m 19.30s (slow by 1m 29.30s)",
                "1m 0.00s (slow by 0.50s)",
                "1m 0.00s (slow by 1.00s)",
                "1m 10.10s (standard time)",
                "59.20s (standard time)",
                "11m 0.20s (fast by 2m 38.20s)",
            ],
            "time_seconds": [61.1, 61.1, 61.1, 61.1, 61.1],
            "relative_to_standard": [np.nan, np.nan, np.nan, np.nan, np.nan],
            "relative_time": [np.nan, np.nan, np.nan, np.nan, np.nan],
        }
    )


def test_create_headgear_data(): ...
