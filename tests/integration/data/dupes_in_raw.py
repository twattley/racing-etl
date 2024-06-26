from datetime import datetime, timedelta

import numpy as np
import pandas as pd

MINUS_5_YEARS = 5 * 365
TEST_DATE = datetime.now().date() - timedelta(days=MINUS_5_YEARS)
TEST_DATETIME = datetime.combine(TEST_DATE, datetime.min.time())

dupes_in_raw = [
    {
        "schema": "rp_raw",
        "table": "performance_data",
        "data": pd.DataFrame(
            {
                "horse_id": ["1", "2", "3", "4", "5", "5"],
                "horse_name": [
                    "Horse A",
                    "Horse B",
                    "Horse C",
                    "Horse D",
                    "Horse E",
                    "Horse E",
                ],
                "horse_type": ["br, m", "b, g", "ch, c", "b, m", "bl, f", "bl, f"],
                "horse_age": ["3", "4", "5", "6", "7", "7"],
                "jockey_id": ["1", "2", "3", "4", "5", "5"],
                "jockey_name": [
                    "Jockey A",
                    "Jockey B",
                    "Jockey C",
                    "Jockey D",
                    "Jockey E",
                    "Jockey E",
                ],
                "jockey_claim": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
                "trainer_id": ["1", "2", "3", "4", "5", "5"],
                "trainer_name": [
                    "Trainer A",
                    "Trainer B",
                    "Trainer C",
                    "Trainer D",
                    "Trainer E",
                    "Trainer E",
                ],
                "owner_id": ["1", "2", "3", "4", "5", "5"],
                "owner_name": [
                    "Owner A",
                    "Owner B",
                    "Owner C",
                    "Owner D",
                    "Owner E",
                    "Owner E",
                ],
                "horse_weight": ["10-0", "10-5", "10-10", "11-0", "11-5", "11-5"],
                "official_rating": ["45", "50", "55", "60", "65", "65"],
                "finishing_position": ["1", "2", "3", "4", "5", "5"],
                "total_distance_beaten": [
                    "",
                    "sht-hd",
                    "3¼",
                    "[4¾]",
                    "[dist]",
                    "[dist]",
                ],
                "draw": ["1", "2", "3", "4", "5", "5"],
                "ts_value": ["40", "45", "50", "55", "60", "60"],
                "rpr_value": ["40", "45", "50", "55", "60", "60"],
                "horse_price": ["2/1", "3/1", "4/1", "5/1", "6/1", "6/1"],
                "extra_weight": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
                "headgear": ["b", np.nan, np.nan, "ht1", np.nan, np.nan],
                "comment": [
                    "Led",
                    "Prominent",
                    "Prominent",
                    "Held up",
                    "Towards rear",
                    "Towards rear",
                ],
                "sire_name": [
                    "Sire A",
                    "Sire B",
                    "Sire C",
                    "Sire D",
                    "Sire E",
                    "Sire E",
                ],
                "sire_id": ["1", "2", "3", "4", "5", "5"],
                "dam_name": [
                    "Dam A",
                    "Dam B",
                    "Dam C",
                    "Dam D",
                    "Dam E",
                    "Dam E",
                ],
                "dam_id": ["1", "2", "3", "4", "5", "5"],
                "dams_sire": [
                    "Dams Sire A",
                    "Dams Sire B",
                    "Dams Sire C",
                    "Dams Sire D",
                    "Dams Sire E",
                    "Dams Sire E",
                ],
                "dams_sire_id": ["1", "2", "3", "4", "5", "5"],
                "race_date": [
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                ],
                "race_title": [
                    "Horse A Race Title",
                    "Horse B Race Title",
                    "Horse C Race Title",
                    "Horse D Race Title",
                    "Horse E Race Title",
                    "Horse E Race Title",
                ],
                "race_time": ["12:00", "12:00", "12:00", "12:00", "12:00", "12:00"],
                "race_timestamp": [
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                ],
                "conditions": [
                    "(4yo+)",
                    "(4yo+)",
                    "(4yo+)",
                    "(4yo+)",
                    "(4yo+)",
                    "(4yo+)",
                ],
                "distance": ["1m1½f", "7½f", "1m1½f", "7½f", "1m1½f", "1m1½f"],
                "distance_full": [
                    "(1m1f100yds)",
                    "(7f92yds)",
                    "(1m1f100yds)",
                    np.nan,
                    np.nan,
                    np.nan,
                ],
                "going": [
                    "Yielding To Soft",
                    "Yielding To Soft",
                    "Yielding To Soft",
                    "Yielding To Soft",
                    "Yielding To Soft",
                    "Yielding To Soft",
                ],
                "winning_time": [
                    "1m 1.10s (slow by 10.00s)",
                    "2m 2.20s (slow by 20.00s)",
                    "3m 3.30s (slow by 30.00s)",
                    "4m 4.40s (slow by 40.00s)",
                    "5m 5.60s (slow by 50.00s)",
                    "5m 5.60s (slow by 50.00s)",
                ],
                "number_of_runners": ["3", "3", "3", "3", "3", "3"],
                "total_prize_money": ["9", "9", "9", "9", "9", "9"],
                "first_place_prize_money": ["6", "6", "6", "6", "6", "6"],
                "currency": ["EURO", "EURO", "EURO", "EURO", "EURO", "EURO"],
                "course_id": ["1", "2", "3", "4", "5", "5"],
                "course_name": [
                    "rp-course-a",
                    "rp-course-b",
                    "rp-course-c",
                    "rp-course-d",
                    "rp-course-e",
                    "rp-course-e",
                ],
                "course": [
                    "course-a",
                    "course-b",
                    "course-c",
                    "course-d",
                    "course-e",
                    "course-e",
                ],
                "race_class": [
                    "(Class 1)",
                    "(Class 2)",
                    "(Class 3)",
                    "(Class 4)",
                    "(Class 5)",
                    np.nan,
                ],
                "debug_link": [
                    "https://www.racingpost.com/results/some_link",
                    "https://www.racingpost.com/results/some_link",
                    "https://www.racingpost.com/results/some_link",
                    "https://www.racingpost.com/results/some_link",
                    "https://www.racingpost.com/results/some_link",
                    "https://www.racingpost.com/results/some_link",
                ],
                "race_id": ["1", "2", "3", "4", "5", "5"],
                "country": ["UK", "UK", "UK", "UK", "UK", "UK"],
                "surface": ["Turf", "Turf", "Turf", "Turf", "Turf", "Turf"],
                "created_at": [
                    TEST_DATETIME + timedelta(hours=10),
                    TEST_DATETIME + timedelta(hours=10),
                    TEST_DATETIME + timedelta(hours=10),
                    TEST_DATETIME + timedelta(hours=10),
                    TEST_DATETIME + timedelta(hours=10),
                    TEST_DATETIME + timedelta(hours=10),
                ],
                "unique_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                    "5",
                ],
                "meeting_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                    "5",
                ],
            }
        ),
    },
    {
        "schema": "tf_raw",
        "table": "performance_data",
        "data": pd.DataFrame(
            {
                "tf_rating": ["+", "?", "p60+", "f70", "a80"],
                "tf_speed_figure": ["30.0", "40.0", "50.0", "60.0", "70.0"],
                "draw": ["(1)", "(2)", "(3)", "(4)", "(5)"],
                "trainer_name": [
                    "Trainer A",
                    "Trainer B",
                    "Trainer C",
                    "Trainer D",
                    "Trainer E",
                ],
                "trainer_id": ["1", "2", "3", "4", "5"],
                "jockey_name": [
                    "Jockey A",
                    "Jockey B",
                    "Jockey C",
                    "Jockey D",
                    "Jockey E",
                ],
                "jockey_id": ["1", "2", "3", "4", "5"],
                "sire_name": [
                    "Sire A",
                    "Sire B",
                    "Sire C",
                    "Sire D",
                    "Sire E",
                ],
                "sire_id": ["1", "2", "3", "4", "5"],
                "dam_name": [
                    "Dam A",
                    "Dam B",
                    "Dam C",
                    "Dam D",
                    "Dam E",
                ],
                "dam_id": ["1", "2", "3", "4", "5"],
                "finishing_position": ["1", "2", "3", "4", "5"],
                "horse_name": [
                    "Horse A",
                    "Horse B",
                    "Horse C",
                    "Horse D",
                    "Horse E",
                ],
                "horse_id": ["1", "2", "3", "4", "5"],
                "horse_name_link": [
                    "horse-a",
                    "horse-b",
                    "horse-c",
                    "horse-d",
                    "horse-e",
                ],
                "horse_age": ["3", "4", "5", "6", "7"],
                "equipment": [np.nan, np.nan, np.nan, np.nan, np.nan],
                "official_rating": ["45", "50", "55", "60", "65"],
                "fractional_price": [np.nan, np.nan, np.nan, np.nan, np.nan],
                "betfair_win_sp": ["3.0", "4.0", "5.0", "6.0", "7.0"],
                "betfair_place_sp": ["2.0", "3.0", "4.0", "5.0", "6.0"],
                "in_play_prices": ["10/-", "-/11", "-/12", "-/13", "-/14"],
                "tf_comment": [
                    "this is a comment for timeform horse a",
                    "this is a comment for timeform horse b",
                    "this is a comment for timeform horse c",
                    "this is a comment for timeform horse d",
                    "this is a comment for timeform horse e",
                ],
                "course": [
                    "tf-course-a",
                    "tf-course-b",
                    "tf-course-c",
                    "tf-course-d",
                    "tf-course-e",
                ],
                "race_date": [
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                ],
                "race_time": ["1200", "1200", "1200", "1200", "1200"],
                "race_timestamp": [
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                    TEST_DATETIME + timedelta(hours=12),
                ],
                "course_id": ["1", "2", "3", "4", "5"],
                "race": ["1", "2", "3", "4", "5"],
                "race_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
                "distance": ["1m 4f 15y", "1m 4f 15y", "1m 2f", "1m 2f", "1m 2f"],
                "going": ["Soft", "Heavy", "Std", "Good", "Good/Soft"],
                "prize": ["£2,000", "£2,000", "£2,000", "£2,000", "£2,000"],
                "hcap_range": ["(0-55)", "(0-60)", "(0-65)", "(0-70)", "(0-75)"],
                "age_range": ["3-4yo", "4yo+", "5yo+", "4yo+", "2yo"],
                "race_type": ["Flat", "Flat", "Hurdle", "Chase", "Flat"],
                "main_race_comment": [
                    "this is the main race comment for timeform horse a",
                    "this is the main race comment for timeform horse b",
                    "this is the main race comment for timeform horse c",
                    "this is the main race comment for timeform horse d",
                    "this is the main race comment for timeform horse e",
                ],
                "debug_link": [
                    "https://www.timeform.com/horse-racing/result_link",
                    "https://www.timeform.com/horse-racing/result_link",
                    "https://www.timeform.com/horse-racing/result_link",
                    "https://www.timeform.com/horse-racing/result_link",
                    "https://www.timeform.com/horse-racing/result_link",
                ],
                "created_at": [
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                ],
                "unique_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
            }
        ),
    },
    {
        "schema": "public",
        "table": "course",
        "data": pd.DataFrame(
            {
                "name": ["Course A", "Course B", "Course C", "Course D", "Course E"],
                "id": [100, 200, 300, 400, 500],
                "rp_name": [
                    "rp-course-a",
                    "rp-course-b",
                    "rp-course-c",
                    "rp-course-d",
                    "rp-course-e",
                ],
                "rp_id": ["1", "2", "3", "4", "5"],
                "tf_name": [
                    "tf-course-a",
                    "tf-course-b",
                    "tf-course-c",
                    "tf-course-d",
                    "tf-course-e",
                ],
                "tf_id": ["1", "2", "3", "4", "5"],
                "country_id": ["1", "1", "1", "1", "1"],
            }
        ),
    },
    {
        "schema": "public",
        "table": "trainer",
        "data": pd.DataFrame(
            {
                "rp_id": [1, 2, 3, 4, 5],
                "name": [
                    "Trainer A",
                    "Trainer B",
                    "Trainer C",
                    "Trainer D",
                    "Trainer E",
                ],
                "tf_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
                "id": [100, 200, 300, 400, 500],
            }
        ),
    },
    {
        "schema": "public",
        "table": "jockey",
        "data": pd.DataFrame(
            {
                "rp_id": [1, 2, 3, 4, 5],
                "name": [
                    "Jockey A",
                    "Jockey B",
                    "Jockey C",
                    "Jockey D",
                    "Jockey E",
                ],
                "tf_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
                "id": [100, 200, 300, 400, 500],
            }
        ),
    },
    {
        "schema": "public",
        "table": "horse",
        "data": pd.DataFrame(
            {
                "rp_id": [1, 2, 3, 4, 5],
                "name": [
                    "Horse A",
                    "Horse B",
                    "Horse C",
                    "Horse D",
                    "Horse E",
                ],
                "tf_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
                "id": [100, 200, 300, 400, 500],
            }
        ),
    },
    {
        "schema": "public",
        "table": "sire",
        "data": pd.DataFrame(
            {
                "rp_id": [1, 2, 3, 4, 5],
                "name": [
                    "Sire A",
                    "Sire B",
                    "Sire C",
                    "Sire D",
                    "Sire E",
                ],
                "tf_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
                "id": [100, 200, 300, 400, 500],
            }
        ),
    },
    {
        "schema": "public",
        "table": "dam",
        "data": pd.DataFrame(
            {
                "rp_id": [1, 2, 3, 4, 5],
                "name": [
                    "Dam A",
                    "Dam B",
                    "Dam C",
                    "Dam D",
                    "Dam E",
                ],
                "tf_id": [
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                ],
                "id": [100, 200, 300, 400, 500],
            }
        ),
    },
    {
        "schema": "public",
        "table": "owner",
        "data": pd.DataFrame(
            {
                "rp_id": [1, 2, 3, 4, 5],
                "name": [
                    "Owner A",
                    "Owner B",
                    "Owner C",
                    "Owner D",
                    "Owner E",
                ],
                "id": [100, 200, 300, 400, 500],
            }
        ),
    },
    {
        "schema": "public",
        "table": "country",
        "data": pd.DataFrame(
            {
                "country_id": [1, 2, 3],
                "country_name": ["UK", "IRE", "FR"],
            }
        ),
    },
]

dupes_in_raw_expected_data = pd.DataFrame(
    {
        "race_time": [
            TEST_DATETIME + timedelta(hours=12),
            TEST_DATETIME + timedelta(hours=12),
            TEST_DATETIME + timedelta(hours=12),
            TEST_DATETIME + timedelta(hours=12),
            TEST_DATETIME + timedelta(hours=12),
        ],
        "race_date": [
            TEST_DATE,
            TEST_DATE,
            TEST_DATE,
            TEST_DATE,
            TEST_DATE,
        ],
        "horse_name": ["Horse A", "Horse B", "Horse C", "Horse D", "Horse E"],
        "age": [3, 4, 5, 6, 7],
        "horse_sex": ["Mare", "Gelding", "Colt", "Mare", "Filly"],
        "draw": [1, 2, 3, 4, 5],
        "headgear": [
            "blinkers",
            np.nan,
            np.nan,
            "tongue tie (first time), hood",
            np.nan,
        ],
        "weight_carried": ["10-0", "10-5", "10-10", "11-0", "11-5"],
        "weight_carried_lbs": [140, 145, 150, 154, 159],
        "extra_weight": [None, None, None, None, None],
        "jockey_claim": [None, None, None, None, None],
        "finishing_position": ["1", "2", "3", "4", "5"],
        "total_distance_beaten": [0.0, 0.1, 3.25, 4.75, 999.0],
        "industry_sp": ["2/1", "3/1", "4/1", "5/1", "6/1"],
        "betfair_win_sp": [3.0, 4.0, 5.0, 6.0, 7.0],
        "betfair_place_sp": [2.0, 3.0, 4.0, 5.0, 6.0],
        "official_rating": [45, 50, 55, 60, 65],
        "ts": [40, 45, 50, 55, 60],
        "rpr": [40, 45, 50, 55, 60],
        "tfr": [np.nan, np.nan, 60.0, 70.0, 80.0],
        "tfig": [30, 40, 50, 60, 70],
        "in_play_high": [10.0, np.nan, np.nan, np.nan, np.nan],
        "in_play_low": [np.nan, 11.0, 12.0, 13.0, 14.0],
        "in_race_comment": ["Led", "Prominent", "Prominent", "Held up", "Towards rear"],
        "tf_comment": [
            "this is a comment for timeform horse a",
            "this is a comment for timeform horse b",
            "this is a comment for timeform horse c",
            "this is a comment for timeform horse d",
            "this is a comment for timeform horse e",
        ],
        "tfr_view": ["positive", "questionable", "positive", "neutral", "neutral"],
        "race_id": [1, 2, 3, 4, 5],
        "course_id": [100, 200, 300, 400, 500],
        "horse_id": [100, 200, 300, 400, 500],
        "jockey_id": [100, 200, 300, 400, 500],
        "trainer_id": [100, 200, 300, 400, 500],
        "owner_id": [100, 200, 300, 400, 500],
        "sire_id": [100, 200, 300, 400, 500],
        "dam_id": [100, 200, 300, 400, 500],
        "unique_id": ["1", "2", "3", "4", "5"],
    }
)
