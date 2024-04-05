import os
import re
import subprocess

import pandas as pd
import pytest
import sqlalchemy

from src.storage.sql_db import fetch_data, storage_connection, store_data

SCHEMA_DIR = os.path.join(os.getcwd(), "src/schema/backup_files")
SCHEMA_FILE = "racehorse-database-schema.sql"
TEST_USER = os.environ["PG_DB_USER"] = "test_user"
TEST_PASSWORD = os.environ["PG_DB_PASSWORD"] = "test_password"
TEST_HOST = os.environ["PG_DB_HOST"] = "localhost"
TEST_PORT = os.environ["PG_DB_PORT"] = "5433"
TEST_DB = os.environ["PG_DB_NAME"] = "test_db"
ADMIN_TEST_DB = os.environ["PG_ADMIN_DB_NAME"] = "postgres"
os.environ["PGPASSWORD"] = "test_password"


@pytest.fixture(scope="function", autouse=True)
def setup_fresh_test_database():
    with storage_connection(db=ADMIN_TEST_DB).connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")

        conn.execute(
            sqlalchemy.text(
                """
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = 'test_db' AND pid <> pg_backend_pid();
            """
            )
        )
        conn.execute(sqlalchemy.text("DROP DATABASE IF EXISTS test_db;"))

        # Ensure the role exists; create if it does not
        if not conn.execute(
            sqlalchemy.text("SELECT 1 FROM pg_roles WHERE rolname='doadmin';")
        ).fetchone():
            conn.execute(sqlalchemy.text("CREATE ROLE doadmin;"))

        # Create a fresh test database
        conn.execute(sqlalchemy.text("CREATE DATABASE test_db OWNER doadmin;"))

        # Grant all privileges on the new database to the test user
        conn.execute(
            sqlalchemy.text(f"GRANT ALL PRIVILEGES ON DATABASE test_db TO {TEST_USER};")
        )

        # Load the schema into the new test database
        try:
            load_schema_command = f"psql -h {TEST_HOST} -p {TEST_PORT} -U {TEST_USER} -d test_db -f {SCHEMA_DIR}/{SCHEMA_FILE}"
            subprocess.run(load_schema_command, check=True, shell=True)
        except subprocess.CalledProcessError as e:
            print(f"An error occurred: {e}")
            raise e

    yield


@pytest.fixture
def load_test_data():
    def _loader(test_data: list[dict]):
        for entry in test_data:
            store_data(entry["data"], entry["table"], entry["schema"])

    return _loader


@pytest.fixture
def fetch_test_data():
    def _fetcher(query: str):
        return fetch_data(query)

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
