from datetime import datetime

import pandas as pd

from src.entity_matching.matcher import entity_match


def test_simple_pass():
    rp_entity_data = pd.DataFrame(
        {
            "horse_name": ["horse_a"],
            "horse_id": [1],
            "horse_age": [3],
            "jockey_id": [1],
            "jockey_name": ["jockey_a"],
            "trainer_id": [1],
            "trainer_name": ["trainer_a"],
            "sire_name": ["sire_a"],
            "sire_id": [1],
            "dam_name": ["dam_a"],
            "dam_id": [1],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
            ],
            "unique_id": [1],
            "race_date": [
                datetime(2021, 1, 1),
            ],
            "debug_link": [
                "https://www.racingpost.com/horses/1/horse_a",
            ],
            "course_id": [1],
            "entity_type": ["horse"],
        }
    )

    tf_matching_data = pd.DataFrame(
        {
            "horse_name": ["horse_a"],
            "horse_id": [1],
            "horse_age": [3],
            "jockey_id": [1],
            "jockey_name": ["jockey_a"],
            "trainer_id": [1],
            "trainer_name": ["trainer_a"],
            "sire_name": ["sire_a"],
            "sire_id": [1],
            "dam_name": ["dam_a"],
            "dam_id": [1],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
            ],
            "unique_id": [1],
            "race_date": [
                datetime(2021, 1, 1),
            ],
            "debug_link": [
                "https://www.timeform.com/horses/1/horse_a",
            ],
            "course_id": [1],
        }
    )

    matched, unmatched = entity_match(tf_matching_data, rp_entity_data)

    expected = pd.DataFrame(
        {
            "entity": ["horse"],
            "rp_id": [1],
            "name": ["Horse_A"],
            "tf_id": [1],
        }
    )

    pd.testing.assert_frame_equal(matched, expected)

    assert unmatched.empty


def test_simple_fail():
    rp_entity_data = pd.DataFrame(
        {
            "horse_name": ["horse_a"],
            "horse_id": [1],
            "horse_age": [3],
            "jockey_id": [1],
            "jockey_name": ["jockey_a"],
            "trainer_id": [1],
            "trainer_name": ["trainer_a"],
            "sire_name": ["sire_a"],
            "sire_id": [1],
            "dam_name": ["dam_a"],
            "dam_id": [1],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
            ],
            "unique_id": [1],
            "race_date": [
                datetime(2021, 1, 1),
            ],
            "debug_link": [
                "https://www.racingpost.com/horses/1/horse_a",
            ],
            "course_id": [1],
            "entity_type": ["horse"],
        }
    )

    tf_matching_data = pd.DataFrame(
        {
            "horse_name": ["different_horse"],
            "horse_id": [1],
            "horse_age": [3],
            "jockey_id": [1],
            "jockey_name": ["jockey_a"],
            "trainer_id": [1],
            "trainer_name": ["trainer_a"],
            "sire_name": ["sire_a"],
            "sire_id": [1],
            "dam_name": ["dam_a"],
            "dam_id": [1],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
            ],
            "unique_id": [1],
            "race_date": [
                datetime(2021, 1, 1),
            ],
            "debug_link": [
                "https://www.timeform.com/horses/1/horse_a",
            ],
            "course_id": [1],
        }
    )

    matched, unmatched = entity_match(tf_matching_data, rp_entity_data)

    expected = pd.DataFrame(
        {
            "entity": ["horse"],
            "race_timestamp": [datetime(2021, 1, 1, 12, 0)],
            "name": ["Horse_A"],
            "debug_link": ["https://www.racingpost.com/horses/1/horse_a"],
        }
    )
    pd.testing.assert_frame_equal(unmatched, expected)

    assert matched.empty


def test_horse_mispelled():
    rp_entity_data = pd.DataFrame(
        {
            "horse_name": ["horse_a"],
            "horse_id": [1],
            "horse_age": [3],
            "jockey_id": [1],
            "jockey_name": ["jockey_a"],
            "trainer_id": [1],
            "trainer_name": ["trainer_a"],
            "sire_name": ["sire_a"],
            "sire_id": [1],
            "dam_name": ["dam_a"],
            "dam_id": [1],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
            ],
            "unique_id": [1],
            "race_date": [
                datetime(2021, 1, 1),
            ],
            "debug_link": [
                "https://www.racingpost.com/horses/1/horse_a",
            ],
            "course_id": [1],
            "entity_type": ["horse"],
        }
    )

    tf_matching_data = pd.DataFrame(
        {
            "horse_name": ["house_a"],  # Mispelled
            "horse_id": [1],
            "horse_age": [3],
            "jockey_id": [1],
            "jockey_name": ["jockey_a"],
            "trainer_id": [1],
            "trainer_name": ["trainer_a"],
            "sire_name": ["sire_a"],
            "sire_id": [1],
            "dam_name": ["dam_a"],
            "dam_id": [1],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
            ],
            "unique_id": [1],
            "race_date": [
                datetime(2021, 1, 1),
            ],
            "debug_link": [
                "https://www.timeform.com/horses/1/horse_a",
            ],
            "course_id": [1],
        }
    )

    matched, unmatched = entity_match(tf_matching_data, rp_entity_data)

    expected = pd.DataFrame(
        {
            "entity": ["horse"],
            "rp_id": [1],
            "name": ["Horse_A"],
            "tf_id": [1],
        }
    )

    pd.testing.assert_frame_equal(matched, expected)

    assert unmatched.empty


def test_dam_mispelled():
    rp_entity_data = pd.DataFrame(
        {
            "horse_name": ["horse_a"],
            "horse_id": [1],
            "horse_age": [3],
            "jockey_id": [1],
            "jockey_name": ["jockey_a"],
            "trainer_id": [1],
            "trainer_name": ["trainer_a"],
            "sire_name": ["sire_a"],
            "sire_id": [1],
            "dam_name": ["dam_a"],
            "dam_id": [1],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
            ],
            "unique_id": [1],
            "race_date": [
                datetime(2021, 1, 1),
            ],
            "debug_link": [
                "https://www.racingpost.com/horses/1/horse_a",
            ],
            "course_id": [1],
            "entity_type": ["dam"],
        }
    )

    tf_matching_data = pd.DataFrame(
        {
            "horse_name": ["horse_a"],
            "horse_id": [1],
            "horse_age": [3],
            "jockey_id": [1],
            "jockey_name": ["jockey_a"],
            "trainer_id": [1],
            "trainer_name": ["trainer_a"],
            "sire_name": ["sire_a"],
            "sire_id": [1],
            "dam_name": ["dam'_a"],  # Mispelled
            "dam_id": [1],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
            ],
            "unique_id": [1],
            "race_date": [
                datetime(2021, 1, 1),
            ],
            "debug_link": [
                "https://www.timeform.com/horses/1/horse_a",
            ],
            "course_id": [1],
        }
    )

    matched, unmatched = entity_match(tf_matching_data, rp_entity_data)

    expected = pd.DataFrame(
        {
            "entity": ["dam"],
            "rp_id": [1],
            "name": ["Dam_A"],
            "tf_id": [1],
        }
    )

    pd.testing.assert_frame_equal(matched, expected)

    assert unmatched.empty


def test_multiple_matchings_wrong_date():
    rp_entity_data = pd.DataFrame(
        {
            "horse_name": ["horse_a", "horse_b", "horse_c"],
            "horse_id": [1, 2, 3],
            "horse_age": [3, 4, 5],
            "jockey_id": [1, 2, 3],
            "jockey_name": ["jockey_a", "jockey_b", "jockey_c"],
            "trainer_id": [1, 2, 3],
            "trainer_name": ["trainer_a", "trainer_b", "trainer_c"],
            "sire_name": ["sire_a", "sire_b", "sire_c"],
            "sire_id": [1, 2, 3],
            "dam_name": ["dam_a", "dam_b", "dam_c"],
            "dam_id": [1, 2, 3],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
                datetime(2021, 1, 2, 12, 0),
                datetime(2021, 1, 3, 12, 0),
            ],
            "unique_id": [1, 2, 3],
            "race_date": [
                datetime(2021, 1, 1),
                datetime(2021, 1, 2),
                datetime(2021, 1, 3),
            ],
            "debug_link": [
                "https://www.racingpost.com/horses/1/horse_a",
                "https://www.racingpost.com/horses/2/horse_b",
                "https://www.racingpost.com/horses/3/horse_c",
            ],
            "course_id": [1, 2, 3],
            "entity_type": ["horse", "sire", "dam"],
        }
    )

    tf_matching_data = pd.DataFrame(
        {
            "horse_name": ["horse_a", "horse_b", "horse_c"],
            "horse_id": [1, 2, 3],
            "horse_age": [3, 4, 5],
            "jockey_id": [1, 2, 3],
            "jockey_name": ["jockey_a", "jockey_b", "jockey_c"],
            "trainer_id": [1, 2, 3],
            "trainer_name": ["trainer_a", "trainer_b", "trainer_c"],
            "sire_name": ["sire_a", "sire_b", "sire_c"],
            "sire_id": [1, 2, 3],
            "dam_name": ["dam_a", "dam_b", "dam_c"],
            "dam_id": [1, 2, 3],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
                datetime(2021, 1, 2, 12, 0),
                datetime(2021, 1, 4, 12, 0),  # Different date
            ],
            "unique_id": [4, 5, 6],
            "race_date": [
                datetime(2021, 1, 1),
                datetime(2021, 1, 2),
                datetime(2021, 1, 4),  # Different date
            ],
            "debug_link": [
                "https://www.timeform.com/horses/1/horse_a",
                "https://www.timeform.com/horses/2/horse_b",
                "https://www.timeform.com/horses/3/horse_c",
            ],
            "course_id": [1, 2, 3],
        }
    )

    expected = pd.DataFrame(
        {
            "entity": ["horse", "sire"],
            "rp_id": [1, 2],
            "name": ["Horse_A", "Sire_B"],
            "tf_id": [1, 2],
        }
    )

    matched, unmatched = entity_match(tf_matching_data, rp_entity_data)

    pd.testing.assert_frame_equal(matched, expected)

    assert unmatched.empty


def test_multiple_matchings_date():
    rp_entity_data = pd.DataFrame(
        {
            "horse_name": ["horse_a", "horse_b", "horse_c"],
            "horse_id": [1, 2, 3],
            "horse_age": [3, 4, 5],
            "jockey_id": [1, 2, 3],
            "jockey_name": ["jockey_a", "jockey_b", "jockey_c"],
            "trainer_id": [1, 2, 3],
            "trainer_name": ["trainer_a", "trainer_b", "trainer_c"],
            "sire_name": ["sire_a", "sire_b", "sire_c"],
            "sire_id": [1, 2, 3],
            "dam_name": ["dam_a", "dam_b", "dam_c"],
            "dam_id": [1, 2, 3],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
                datetime(2021, 1, 2, 12, 0),
                datetime(2021, 1, 3, 12, 0),
            ],
            "unique_id": [1, 2, 3],
            "race_date": [
                datetime(2021, 1, 1),
                datetime(2021, 1, 2),
                datetime(2021, 1, 3),
            ],
            "debug_link": [
                "https://www.racingpost.com/horses/1/horse_a",
                "https://www.racingpost.com/horses/2/horse_b",
                "https://www.racingpost.com/horses/3/horse_c",
            ],
            "course_id": [1, 2, 3],
            "entity_type": ["horse", "sire", "dam"],
        }
    )

    tf_matching_data = pd.DataFrame(
        {
            "horse_name": ["horse_a", "horse_b", "horse_c"],
            "horse_id": [1, 2, 3],
            "horse_age": [3, 4, 5],
            "jockey_id": [1, 2, 3],
            "jockey_name": ["jockey_a", "jockey_b", "jockey_c"],
            "trainer_id": [1, 2, 3],
            "trainer_name": ["trainer_a", "trainer_b", "trainer_c"],
            "sire_name": ["sire_a", "sire_b", "sire_c"],
            "sire_id": [1, 2, 3],
            "dam_name": ["dam_a", "dam_b", "wrong_name"],  # Wrong name
            "dam_id": [1, 2, 3],
            "race_timestamp": [
                datetime(2021, 1, 1, 12, 0),
                datetime(2021, 1, 2, 12, 0),
                datetime(2021, 1, 3, 12, 0),
            ],
            "unique_id": [4, 5, 6],
            "race_date": [
                datetime(2021, 1, 1),
                datetime(2021, 1, 2),
                datetime(2021, 1, 3),
            ],
            "debug_link": [
                "https://www.timeform.com/horses/1/horse_a",
                "https://www.timeform.com/horses/2/horse_b",
                "https://www.timeform.com/horses/3/horse_c",
            ],
            "course_id": [1, 2, 3],
        }
    )

    expected = pd.DataFrame(
        {
            "entity": ["horse", "sire"],
            "rp_id": [1, 2],
            "name": ["Horse_A", "Sire_B"],
            "tf_id": [1, 2],
        }
    )

    expected_unmatched = pd.DataFrame(
        {
            "entity": ["dam"],
            "race_timestamp": [datetime(2021, 1, 3, 12, 0)],
            "name": ["Dam_C"],
            "debug_link": ["https://www.racingpost.com/horses/3/horse_c"],
        }
    )

    matched, unmatched = entity_match(tf_matching_data, rp_entity_data)

    pd.testing.assert_frame_equal(matched, expected)
    pd.testing.assert_frame_equal(unmatched, expected_unmatched)
