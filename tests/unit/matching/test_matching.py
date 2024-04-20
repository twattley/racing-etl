import pandas as pd
import pytest

from src.entity_matching.matcher import MatchingData, fuzzy_match_entities


@pytest.fixture
def base_empty_matching_data():
    return pd.DataFrame(
        {
            "horse_name": [],
            "filtered_horse_name": [],
            "horse_id": [],
            "jockey_id": [],
            "jockey_name": [],
            "filtered_jockey_name": [],
            "trainer_id": [],
            "trainer_name": [],
            "filtered_trainer_name": [],
            "converted_finishing_position": [],
            "sire_name": [],
            "filtered_sire_name": [],
            "sire_id": [],
            "dam_name": [],
            "filtered_dam_name": [],
            "dam_id": [],
            "race_timestamp": [],
            "course_id": [],
            "unique_id": [],
            "race_date": [],
            "debug_link": [],
        }
    )


@pytest.fixture
def base_matching_data():
    return pd.DataFrame(
        {
            "horse_name": ["Horse A"],
            "filtered_horse_name": ["horsea"],
            "horse_id": ["1"],
            "jockey_id": ["1"],
            "jockey_name": ["Jockey A"],
            "filtered_jockey_name": ["jockeya"],
            "trainer_id": ["1"],
            "trainer_name": ["Trainer A"],
            "filtered_trainer_name": ["trainera"],
            "sire_name": ["Sire A"],
            "filtered_sire_name": ["sirea"],
            "sire_id": ["1"],
            "dam_name": ["Dam A"],
            "filtered_dam_name": ["damA"],
            "dam_id": ["1"],
            "race_timestamp": [
                pd.Timestamp("2010-01-01 12:00:00"),
            ],
            "course_id": [1],
            "unique_id": ["1"],
            "race_date": [
                pd.Timestamp("2010-01-01"),
            ],
            "debug_link": ["http://www.test_debug_link.com"],
        }
    )


@pytest.fixture
def base_data():
    return pd.DataFrame(
        {
            "horse_name": [
                "Horse A",
                "Horse B",
                "Horse C",
                "Horse D",
                "Horse E",
            ],
            "filtered_horse_name": [
                "horsea",
                "horseb",
                "horser",
                "horsed",
                "horsee",
            ],
            "horse_id": ["1", "2", "3", "4", "5"],
            "jockey_id": ["1", "2", "3", "4", "5"],
            "jockey_name": [
                "Jockey A",
                "Jockey B",
                "Jockey C",
                "Jockey D",
                "Jockey E",
            ],
            "filtered_jockey_name": [
                "jockeya",
                "jockeyb",
                "jockeyc",
                "jockeyd",
                "jockeye",
            ],
            "trainer_id": ["1", "2", "3", "4", "5"],
            "trainer_name": [
                "Trainer A",
                "Trainer B",
                "Trainer C",
                "Trainer D",
                "Trainer E",
            ],
            "filtered_trainer_name": [
                "trainera",
                "trainerb",
                "trainerc",
                "trainerd",
                "trainere",
            ],
            "sire_name": ["Sire A", "Sire B", "Sire C", "Sire D", "Sire E"],
            "filtered_sire_name": [
                "sirea",
                "sireb",
                "sirec",
                "sired",
                "siree",
            ],
            "sire_id": ["1", "2", "3", "4", "5"],
            "dam_name": ["Dam A", "Dam B", "Dam C", "Dam D", "Dam E"],
            "filtered_dam_name": ["damA", "damB", "damC", "damD", "damE"],
            "dam_id": ["1", "2", "3", "4", "5"],
            "race_timestamp": [
                pd.Timestamp("2010-01-01 12:00:00"),
                pd.Timestamp("2010-01-02 12:00:00"),
                pd.Timestamp("2010-01-03 12:00:00"),
                pd.Timestamp("2010-01-04 12:00:00"),
                pd.Timestamp("2010-01-05 12:00:00"),
            ],
            "course_id": [1, 2, 3, 4, 5],
            "unique_id": ["1", "2", "3", "4", "5"],
            "race_date": [
                pd.Timestamp("2010-01-01"),
                pd.Timestamp("2010-01-02"),
                pd.Timestamp("2010-01-03"),
                pd.Timestamp("2010-01-04"),
                pd.Timestamp("2010-01-05"),
            ],
        }
    )


@pytest.mark.parametrize(
    "entity, expected_output",
    [
        (
            "sire",
            pd.DataFrame(
                {
                    "entity": ["sire"],
                    "rp_id": ["1"],
                    "name": ["Sire A"],
                    "tf_id": ["1"],
                }
            ),
        ),
        (
            "dam",
            pd.DataFrame(
                {
                    "entity": ["dam"],
                    "rp_id": ["1"],
                    "name": ["Dam A"],
                    "tf_id": ["1"],
                }
            ),
        ),
        (
            "horse",
            pd.DataFrame(
                {
                    "entity": ["horse"],
                    "rp_id": ["1"],
                    "name": ["Horse A"],
                    "tf_id": ["1"],
                }
            ),
        ),
        (
            "jockey",
            pd.DataFrame(
                {
                    "entity": ["jockey"],
                    "rp_id": ["1"],
                    "name": ["Jockey A"],
                    "tf_id": ["1"],
                }
            ),
        ),
        (
            "trainer",
            pd.DataFrame(
                {
                    "entity": ["trainer"],
                    "rp_id": ["1"],
                    "name": ["Trainer A"],
                    "tf_id": ["1"],
                }
            ),
        ),
    ],
)
def test_simple_pass(
    base_data, base_empty_matching_data, base_matching_data, entity, expected_output
):
    test_entities = [
        ("sire", base_matching_data),
        ("dam", base_matching_data),
        ("horse", base_matching_data),
        ("jockey", base_matching_data),
        ("trainer", base_matching_data),
    ]
    empty_entities = [
        ("sire", base_empty_matching_data),
        ("dam", base_empty_matching_data),
        ("horse", base_empty_matching_data),
        ("jockey", base_empty_matching_data),
        ("trainer", base_empty_matching_data),
    ]
    test_entity = [x for x in test_entities if x[0] == entity][0]
    empty_entities = [x for x in empty_entities if x[0] != entity]

    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                *empty_entities,
                test_entity,
            ],
        )
    )

    pd.testing.assert_frame_equal(matched, expected_output)

    assert unmatched.empty


def test_sire_mispelled(base_empty_matching_data):
    base_data = pd.DataFrame(
        {
            "horse_name": ["Horse A"],
            "filtered_horse_name": ["horsea"],
            "horse_id": ["1"],
            "jockey_id": ["1"],
            "jockey_name": ["Jockey A"],
            "filtered_jockey_name": ["jockeya"],
            "trainer_id": ["1"],
            "trainer_name": ["Trainer A"],
            "filtered_trainer_name": ["trainera"],
            "sire_name": ["Sire's A"],
            "filtered_sire_name": ["sire's a"],
            "sire_id": ["1"],
            "dam_name": ["Dam A"],
            "filtered_dam_name": ["damA"],
            "dam_id": ["1"],
            "race_timestamp": [
                pd.Timestamp("2010-01-01 12:00:00"),
            ],
            "course_id": [1],
            "unique_id": ["1"],
            "race_date": [
                pd.Timestamp("2010-01-01"),
            ],
            "debug_link": [None],
        }
    )

    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                ("sire", base_data),
                ("dam", base_empty_matching_data),
                ("horse", base_empty_matching_data),
                ("jockey", base_empty_matching_data),
                ("trainer", base_empty_matching_data),
            ],
        )
    )

    expected = pd.DataFrame(
        {
            "entity": ["sire"],
            "rp_id": ["1"],
            "name": ["Sires A"],
            "tf_id": ["1"],
        }
    )
    pd.testing.assert_frame_equal(matched, expected)
    assert unmatched.empty


def test_dam_mispelled(base_empty_matching_data):
    base_data = pd.DataFrame(
        {
            "horse_name": ["Horse A"],
            "filtered_horse_name": ["horsea"],
            "horse_id": ["1"],
            "jockey_id": ["1"],
            "jockey_name": ["Jockey A"],
            "filtered_jockey_name": ["jockeya"],
            "trainer_id": ["1"],
            "trainer_name": ["Trainer A"],
            "filtered_trainer_name": ["trainera"],
            "sire_name": ["Sire A"],
            "filtered_sire_name": ["sire a"],
            "sire_id": ["1"],
            "dam_name": ["Dam's A"],
            "filtered_dam_name": ["dam's a"],
            "dam_id": ["1"],
            "race_timestamp": [
                pd.Timestamp("2010-01-01 12:00:00"),
            ],
            "course_id": [1],
            "unique_id": ["1"],
            "race_date": [
                pd.Timestamp("2010-01-01"),
            ],
            "debug_link": [None],
        }
    )

    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                ("dam", base_data),
                ("sire", base_empty_matching_data),
                ("horse", base_empty_matching_data),
                ("jockey", base_empty_matching_data),
                ("trainer", base_empty_matching_data),
            ],
        )
    )

    expected = pd.DataFrame(
        {
            "entity": ["dam"],
            "rp_id": ["1"],
            "name": ["Dams A"],
            "tf_id": ["1"],
        }
    )
    pd.testing.assert_frame_equal(matched, expected)
    assert unmatched.empty


def test_trainer_mispelled(base_empty_matching_data):
    base_data = pd.DataFrame(
        {
            "horse_name": ["Horse A"],
            "filtered_horse_name": ["horsea"],
            "horse_id": ["1"],
            "jockey_id": ["1"],
            "jockey_name": ["Jockey A"],
            "filtered_jockey_name": ["jockeya"],
            "trainer_id": ["1"],
            "trainer_name": ["Trainer's A (IRE)"],
            "filtered_trainer_name": ["trainer's a (ire)"],
            "sire_name": ["Sire A"],
            "filtered_sire_name": ["sire a"],
            "sire_id": ["1"],
            "dam_name": ["Dam A"],
            "filtered_dam_name": ["dam a"],
            "dam_id": ["1"],
            "race_timestamp": [
                pd.Timestamp("2010-01-01 12:00:00"),
            ],
            "course_id": [1],
            "unique_id": ["1"],
            "race_date": [
                pd.Timestamp("2010-01-01"),
            ],
            "debug_link": [None],
        }
    )

    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                ("trainer", base_data),
                ("sire", base_empty_matching_data),
                ("horse", base_empty_matching_data),
                ("jockey", base_empty_matching_data),
                ("dam", base_empty_matching_data),
            ],
        )
    )

    expected = pd.DataFrame(
        {
            "entity": ["trainer"],
            "rp_id": ["1"],
            "name": ["Trainers A"],
            "tf_id": ["1"],
        }
    )
    pd.testing.assert_frame_equal(matched, expected)
    assert unmatched.empty


def test_jockey_mispelled(base_empty_matching_data):
    base_data = pd.DataFrame(
        {
            "horse_name": ["Horse A"],
            "filtered_horse_name": ["horsea"],
            "horse_id": ["1"],
            "jockey_id": ["1"],
            "jockey_name": ["Jockey's A (FR)"],
            "filtered_jockey_name": ["jockey's a (fr)"],
            "trainer_id": ["1"],
            "trainer_name": ["Trainer's A (IRE)"],
            "filtered_trainer_name": ["trainer's a (ire)"],
            "sire_name": ["Sire A"],
            "filtered_sire_name": ["sire a"],
            "sire_id": ["1"],
            "dam_name": ["Dam A"],
            "filtered_dam_name": ["dam a"],
            "dam_id": ["1"],
            "race_timestamp": [
                pd.Timestamp("2010-01-01 12:00:00"),
            ],
            "course_id": [1],
            "unique_id": ["1"],
            "race_date": [
                pd.Timestamp("2010-01-01"),
            ],
            "debug_link": [None],
        }
    )

    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                ("jockey", base_data),
                ("sire", base_empty_matching_data),
                ("horse", base_empty_matching_data),
                ("jockey", base_empty_matching_data),
                ("dam", base_empty_matching_data),
            ],
        )
    )

    expected = pd.DataFrame(
        {
            "entity": ["jockey"],
            "rp_id": ["1"],
            "name": ["Jockeys A"],
            "tf_id": ["1"],
        }
    )
    pd.testing.assert_frame_equal(matched, expected)
    assert unmatched.empty


def test_different_jockeys(base_data, base_empty_matching_data, base_matching_data):
    base_matching_data = base_matching_data.assign(
        jockey_name="Jockey B",
        filtered_jockey_name="jockeyb",
        debug_link="http://www.missing_jockeyb.com",
    )
    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                ("jockey", base_matching_data),
                ("sire", base_empty_matching_data),
                ("horse", base_empty_matching_data),
                ("jockey", base_empty_matching_data),
                ("dam", base_empty_matching_data),
            ],
        )
    )

    expected_unmatched = pd.DataFrame(
        {
            "entity": ["jockey"],
            "race_timestamp": [pd.Timestamp("2010-01-01 12:00:00")],
            "name": ["Jockey B"],
            "debug_link": ["http://www.missing_jockeyb.com"],
        }
    )
    pd.testing.assert_frame_equal(unmatched, expected_unmatched)
    assert matched.empty


def test_different_trainers(base_data, base_empty_matching_data, base_matching_data):
    base_matching_data = base_matching_data.assign(
        trainer_name="Trainer B",
        filtered_trainer_name="trainerb",
        debug_link="http://www.missing_trainerb.com",
    )
    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                ("trainer", base_matching_data),
                ("sire", base_empty_matching_data),
                ("horse", base_empty_matching_data),
                ("trainer", base_empty_matching_data),
                ("dam", base_empty_matching_data),
            ],
        )
    )

    expected_unmatched = pd.DataFrame(
        {
            "entity": ["trainer"],
            "race_timestamp": [pd.Timestamp("2010-01-01 12:00:00")],
            "name": ["Trainer B"],
            "debug_link": ["http://www.missing_trainerb.com"],
        }
    )
    pd.testing.assert_frame_equal(unmatched, expected_unmatched)
    assert matched.empty


def test_multiple_matched(base_data, base_matching_data, base_empty_matching_data):
    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                ("trainer", base_matching_data),
                ("sire", base_matching_data),
                ("horse", base_empty_matching_data),
                ("trainer", base_empty_matching_data),
                ("dam", base_empty_matching_data),
            ],
        )
    )

    expected = pd.DataFrame(
        {
            "entity": ["trainer", "sire"],
            "rp_id": ["1", "1"],
            "name": ["Trainer A", "Sire A"],
            "tf_id": ["1", "1"],
        }
    )
    pd.testing.assert_frame_equal(matched, expected)
    assert unmatched.empty


def test_multiple_unmatched(base_data, base_matching_data, base_empty_matching_data):
    trainer_base_matching_data = base_matching_data.assign(
        trainer_name="Trainer B",
        filtered_trainer_name="trainerb",
        debug_link="http://www.missing_trainerb.com",
    )
    sire_base_matching_data = base_matching_data.assign(
        sire_name="Sire B",
        filtered_sire_name="sireb",
        debug_link="http://www.missing_sireb.com",
    )
    matched, unmatched = fuzzy_match_entities(
        MatchingData(
            base_set="RP",
            base_data=base_data,
            entities_sets=[
                ("trainer", trainer_base_matching_data),
                ("sire", sire_base_matching_data),
                ("horse", base_empty_matching_data),
                ("trainer", base_empty_matching_data),
                ("dam", base_empty_matching_data),
            ],
        )
    )

    expected_unmatched = pd.DataFrame(
        {
            "entity": ["trainer", "sire"],
            "race_timestamp": [
                pd.Timestamp("2010-01-01 12:00:00"),
                pd.Timestamp("2010-01-01 12:00:00"),
            ],
            "name": ["Trainer B", "Sire B"],
            "debug_link": [
                "http://www.missing_trainerb.com",
                "http://www.missing_sireb.com",
            ],
        }
    )
    pd.testing.assert_frame_equal(unmatched, expected_unmatched)
    assert matched.empty
