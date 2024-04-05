from src.pipelines.matching_pipeline import run_matching_pipeline
from src.pipelines.transform_pipeline import run_transform_pipeline
from tests.integration.data.dupes_in_raw import dupes_in_raw, dupes_in_raw_expected_data
from tests.integration.data.jockey_match import jockey_match, jockey_match_expected_data
from tests.integration.data.missing_entity import missing_entity
from tests.integration.data.new_data_in_raw import (
    new_data_in_raw,
    new_data_in_raw_expected_data,
)
from tests.integration.data.simple_pass import simple_pass, simple_pass_expected_data


def test_simple_pass(load_test_data, fetch_test_data, assert_data_equal):
    load_test_data(simple_pass)
    run_matching_pipeline()
    run_transform_pipeline()
    output_data = fetch_test_data("SELECT * FROM performance_data")
    assert_data_equal(output_data, simple_pass_expected_data, ["horse_id"])


def test_filters_dupes_in_raw(load_test_data, fetch_test_data, assert_data_equal):
    load_test_data(dupes_in_raw)
    run_matching_pipeline()
    run_transform_pipeline()
    output_data = fetch_test_data("SELECT * FROM performance_data")
    assert_data_equal(output_data, dupes_in_raw_expected_data, ["horse_id"])


def test_loads_new_data(load_test_data, fetch_test_data, assert_data_equal):
    load_test_data(new_data_in_raw)
    run_matching_pipeline()
    run_transform_pipeline()
    output_data = fetch_test_data("SELECT * FROM performance_data")
    assert_data_equal(output_data, new_data_in_raw_expected_data, ["horse_id"])


def test_adds_missing_entity(load_test_data, fetch_test_data, assert_data_equal):
    load_test_data(missing_entity)
    run_matching_pipeline()
    run_transform_pipeline()
    output_data = fetch_test_data("SELECT * FROM performance_data")
    assert len(output_data) == 5
    rejected_data = fetch_test_data(
        "SELECT * FROM staging.transformed_performance_data_rejected"
    )
    assert len(rejected_data) == 0


def test_jockey_match(load_test_data, fetch_test_data, assert_data_equal):
    load_test_data(jockey_match)
    run_matching_pipeline()
    run_transform_pipeline()
    output_data = fetch_test_data("SELECT * FROM performance_data")
    assert_data_equal(output_data, jockey_match_expected_data, ["horse_id"])
