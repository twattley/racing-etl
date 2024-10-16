import numpy as np
import pandas as pd

from src.transform.transform_data import (
    convert_horse_type_to_colour_sex,
    convert_tf_rating,
    create_distance_beaten_data,
    create_distance_data,
    create_headgear_data,
    create_pounds,
    create_time_data,
    get_inplay_high_and_low,
    get_surfaces_from_tf_rating,
    get_tf_rating_values,
)


def test_tf_rating_values():
    input_df = pd.DataFrame(
        {
            "tfr": [
                "",
                "+",
                "?",
                "1",
                "1+",
                "1?",
                "a1",
                "a1+",
                "a1?",
                "p1",
                "p1+",
                "p1?",
                "f1",
                "f1+",
                "f1?",
            ],
            "data_type": ["historical"] * 15,
        }
    )

    output_df = (
        input_df.pipe(get_surfaces_from_tf_rating)
        .pipe(get_tf_rating_values)
        .pipe(convert_tf_rating)
    )

    expected_df = pd.DataFrame(
        {
            "tfr": [np.nan, np.nan, np.nan, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "surface": [
                "turf",
                "turf",
                "turf",
                "turf",
                "turf",
                "turf",
                "artificial",
                "artificial",
                "artificial",
                "polytrack",
                "polytrack",
                "polytrack",
                "fibresand",
                "fibresand",
                "fibresand",
            ],
            "tfr_view": [
                "neutral",
                "positive",
                "questionable",
                "neutral",
                "positive",
                "questionable",
                "neutral",
                "positive",
                "questionable",
                "neutral",
                "positive",
                "questionable",
                "neutral",
                "positive",
                "questionable",
            ],
            "data_type": ["historical"] * 15,
        }
    )

    expected_df["tfr"] = expected_df["tfr"].astype("Int64")

    pd.testing.assert_frame_equal(
        output_df.sort_index(axis=1).reset_index(drop=True),
        expected_df.sort_index(axis=1).reset_index(drop=True),
    )


def test_create_pounds():
    input_df = pd.DataFrame({"weight_carried": ["8-11", "9-11", "10-11", ""]})
    output_df = input_df.pipe(create_pounds)

    expected_df = pd.DataFrame(
        {
            "weight_carried": ["8-11", "9-11", "10-11", ""],
            "weight_carried_st": [8, 9, 10, np.nan],
            "weight_carried_extra_lbs": [11, 11, 11, np.nan],
            "weight_carried_lbs": [123, 137, 151, np.nan],
        }
    )
    for i in [
        "weight_carried_st",
        "weight_carried_extra_lbs",
        "weight_carried_lbs",
    ]:
        expected_df[i] = expected_df[i].astype("Int64")

    pd.testing.assert_frame_equal(output_df, expected_df)


def test_get_inplay_high_and_low():
    input_df = pd.DataFrame(
        {
            "in_play_prices": ["-/2", "2/-", "/-", "/", ""],
            "data_type": ["historical"] * 5,
        }
    )
    output_df = input_df.pipe(get_inplay_high_and_low)

    pd.testing.assert_frame_equal(
        output_df,
        pd.DataFrame(
            {
                "in_play_prices": ["-/2", "2/-", "/-", "/", ""],
                "data_type": ["historical"] * 5,
                "in_play_low": [2.0, np.nan, np.nan, np.nan, np.nan],
                "in_play_high": [np.nan, 2.0, np.nan, np.nan, np.nan],
            }
        ),
    )


def test_create_distance_data():
    input_df = pd.DataFrame(
        {
            "distance": ["1m1½f", "7½f", "1m1⅛f", "6½f", "6f", "1m1f", "", np.nan],
            "distance_full": [
                "(1m1f100yds)",
                "(7f92yds)",
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ],
        }
    )

    output_df = input_df.pipe(create_distance_data)

    expected_df = pd.DataFrame(
        {
            "distance": ["1m1½f", "7½f", "1m1⅛f", "6½f", "6f", "1m1f", "", np.nan],
            "distance_full": [
                "(1m1f100yds)",
                "(7f92yds)",
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
            ],
            "distance_yards": [
                2090.0,
                1650.0,
                np.nan,
                1430.0,
                1320.0,
                1980.0,
                np.nan,
                np.nan,
            ],
            "distance_meters": [
                1911.096,
                1508.76,
                np.nan,
                1307.592,
                1207.008,
                1810.512,
                np.nan,
                np.nan,
            ],
            "distance_kilometers": [
                1.911096,
                1.50876,
                np.nan,
                1.307592,
                1.207008,
                1.810512,
                np.nan,
                np.nan,
            ],
        }
    )
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_create_time_data():
    input_df = pd.DataFrame(
        {
            "winning_time": [
                "",
                "0.00s",
                "0.00s (standard time)",
                "10m 19.30s (slow by 1m 30.00s)",
                "1m 0.00s (slow by 0.50s)",
                "1m 0.00s (slow by 1.00s)",
                "1m 10.10s (standard time)",
                "59.20s (standard time)",
                "11m 0.20s (fast by 2m 30.00s)",
            ],
            "data_type": ["historical"] * 9,
        }
    )
    output_df = input_df.pipe(create_time_data)

    expected_df = pd.DataFrame(
        {
            "winning_time": [
                "",
                "0.00s",
                "0.00s (standard time)",
                "10m 19.30s (slow by 1m 30.00s)",
                "1m 0.00s (slow by 0.50s)",
                "1m 0.00s (slow by 1.00s)",
                "1m 10.10s (standard time)",
                "59.20s (standard time)",
                "11m 0.20s (fast by 2m 30.00s)",
            ],
            "data_type": ["historical"] * 9,
            "time_seconds": [
                np.nan,
                np.nan,
                np.nan,
                619.3,
                60.0,
                60.0,
                70.1,
                59.2,
                660.2,
            ],
            "relative_to_standard": [
                np.nan,
                np.nan,
                np.nan,
                "slow",
                "slow",
                "slow",
                "standard",
                "standard",
                "fast",
            ],
            "relative_time": [np.nan, np.nan, np.nan, 90.0, 0.5, 1.0, 0.0, 0.0, 150.0],
        }
    )

    pd.testing.assert_frame_equal(output_df, expected_df)


def test_create_headgear_data():
    input_df = pd.DataFrame(
        {
            "headgear": [
                "tb1",
                "esb",
                "hvcp1",
                "b1",
                "e/s1",
                "e/sp1",
                "ep1",
                "btpe1",
                "",
            ]
        }
    )

    output_df = input_df.pipe(create_headgear_data)

    expected_df = pd.DataFrame(
        {
            "headgear": [
                "blinkers (first time), tongue tie",
                "blinkers, eye shield",
                "cheekpieces (first time), visor, hood",
                "blinkers (first time)",
                "eye shield (first time)",
                "cheekpieces (first time), eye shield",
                "cheekpieces (first time), eye shield",
                "eye shield (first time), blinkers, tongue tie, cheekpieces",
                np.nan,
            ],
        }
    )

    pd.testing.assert_frame_equal(output_df, expected_df)


def test_distance_beaten_data():
    input_df = pd.DataFrame(
        {
            "race_id": ["1", "1", "1", "2", "2", "2", "3", "3", "3", "3"],
            "total_distance_beaten": [
                "",
                "sht-hd",
                "3¼",
                "3",
                "",
                "[4¾]",
                "",
                "2¼",
                "[4]",
                "[dist]",
            ],
            "finishing_position": ["1", "2", "3", "2", "1", "3", "1", "2", "3", "4"],
            "data_type": ["historical"] * 10,
        }
    )

    output_df = input_df.pipe(create_distance_beaten_data)

    expected_df = pd.DataFrame(
        {
            "race_id": ["1", "1", "1", "2", "2", "2", "3", "3", "3", "3"],
            "total_distance_beaten": [
                -0.0,
                0.1,
                3.25,
                3.0,
                0.0,
                4.75,
                0.0,
                2.25,
                4.0,
                999.0,
            ],
            "finishing_position": ["1", "2", "3", "2", "1", "3", "1", "2", "3", "4"],
            "data_type": ["historical"] * 10,
            "total_distance_beaten_str": [
                "",
                "sht-hd",
                "3¼",
                "3",
                "",
                "[4¾]",
                "",
                "2¼",
                "[4]",
                "[dist]",
            ],
        }
    )
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_convert_horse_type_to_colour_sex():
    input_df = pd.DataFrame(
        {
            "horse_type": [
                "b, g",
                "b, g",
                "ch, g",
                "ch, f",
                "ch, g",
                "ch, m",
                "gr, c",
                "b, g",
                "ch, g",
                "b, g",
            ],
            "data_type": ["historical"] * 10,
        }
    )

    output_df = input_df.pipe(convert_horse_type_to_colour_sex)

    expected_df = pd.DataFrame(
        {
            "data_type": ["historical"] * 10,
            "horse_colour": [
                "Bay",
                "Bay",
                "Chestnut",
                "Chestnut",
                "Chestnut",
                "Chestnut",
                "Grey",
                "Bay",
                "Chestnut",
                "Bay",
            ],
            "horse_sex": [
                "Gelding",
                "Gelding",
                "Gelding",
                "Filly",
                "Gelding",
                "Mare",
                "Colt",
                "Gelding",
                "Gelding",
                "Gelding",
            ],
        }
    )

    pd.testing.assert_frame_equal(output_df, expected_df)
