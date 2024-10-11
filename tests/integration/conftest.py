import os

os.environ["ENV"] = "TEST"
import re
import subprocess

import pandas as pd
import pytest

from src.storage.storage_client import get_storage_client

db = get_storage_client("postgres")


def run_script(script_path: str):
    try:
        subprocess.run(script_path, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running the script: {e}")
        raise e


@pytest.fixture(scope="function", autouse=True)
def setup_fresh_test_database():
    # run_script("./tests/integration/drop_test_db.sh")
    run_script("./tests/integration/setup_test_db.sh")
    yield


@pytest.fixture
def load_test_data():
    def _loader(test_data: list[dict]):
        for entry in test_data:
            db.store_data(entry["data"], entry["table"], entry["schema"])

    return _loader


@pytest.fixture
def fetch_test_data():
    def _fetcher(query: str):
        return db.fetch_data(query)

    return _fetcher


@pytest.fixture
def assert_data_equal():
    def _assert(data1: pd.DataFrame, data2: pd.DataFrame, sort_cols: list[str] = []):
        if "created_at" in data1.columns:
            data1.drop(columns=["created_at"], inplace=True)
        if "created_at" in data2.columns:
            data2.drop(columns=["created_at"], inplace=True)
        if sort_cols:
            data1 = data1.sort_values(by=sort_cols)
            data2 = data2.sort_values(by=sort_cols)
        pd.testing.assert_frame_equal(
            data1.reset_index(drop=True),
            data2.reset_index(drop=True),
        )

    return _assert


def truncate_table_statements():
    SCHEMA_DIR = os.path.join(os.getcwd(), "src/schema/backup_files")
    SCHEMA_FILE = "racehorse-database-schema.sql"

    with open(f"{SCHEMA_DIR}/{SCHEMA_FILE}", "r", encoding="utf-8") as file:
        schema_content = file.read()

    pattern = re.compile(
        r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(([\w]+)\.)?([\w]+)\s", re.DOTALL
    )
    matches = pattern.findall(schema_content)

    for match in matches:
        schema_name = match[1] or "public"
        table_name = match[2]
        yield f"TRUNCATE TABLE {schema_name}.{table_name} CASCADE;"
