import itertools
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd

from src.config import config
from src.storage.psql_db import get_db
from src.storage.s3_bucket import DigitalOceanSpacesHandler

db = get_db()
from src.utils.logging_config import I

RUNNING_TIME = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def fetch_entity_data(table: str):
    return db.fetch_data(
        f"""
        SELECT * 
        FROM public.{table}
        """
    )


def upload_entity_data(
    table: str, digital_ocean_handler: DigitalOceanSpacesHandler
) -> None:
    I(f"Fetching data for public.{table}")
    df = fetch_entity_data(table)  # Fetch data
    if not df.empty:
        folder = f"snapshots/{RUNNING_TIME}/public/{table}"
        file_name = "entity_data_chunk.parquet"
        object_path = f"{folder}/{file_name}"
        I(f"Uploading data to {object_path}")
        success = digital_ocean_handler.upload_df_as_parquet(df, object_path)
        if success:
            I(f"Successfully uploaded {object_path}")
        else:
            I(f"Failed to upload {object_path}")
    else:
        I(f"No data to upload for public.{table}")


def fetch_performance_data(schema: str, table: str, year: int) -> pd.DataFrame:
    return db.fetch_data(
        f"""
        SELECT * 
        FROM {schema}.{table}
        WHERE EXTRACT(YEAR FROM race_date::date) = {year};
        """
    )


def upload_performance_data(
    schema: str, table: str, year: int, digital_ocean_handler: DigitalOceanSpacesHandler
) -> None:
    I(f"Fetching data for {schema}.{table} for year {year}")
    df = fetch_performance_data(schema, table, year)  # Fetch data
    if not df.empty:
        folder = f"snapshots/{RUNNING_TIME}/{schema}/{table}/{year}"
        file_name = "performance_data_chunk.parquet"
        object_path = f"{folder}/{file_name}"
        I(f"Uploading data to {object_path}")
        success = digital_ocean_handler.upload_df_as_parquet(
            df, object_path
        )  # Upload data
        if success:
            I(f"Successfully uploaded {object_path}")
        else:
            I(f"Failed to upload {object_path}")
    else:
        I(f"No data to upload for {schema}.{table} in {year}")


if __name__ == "__main__":
    subprocess.run(
        [
            "pg_dump",
            "-h",
            config.pg_db_host,
            "-U",
            config.pg_db_user,
            "-s",
            "-f",
            "./src/schema/backup_files/racehorse-database-schema.sql",
            config.pg_db_name,
        ]
    )

    d = DigitalOceanSpacesHandler(
        access_key_id=config.digital_ocean_spaces_access_key_id,
        secret_access_key=config.digital_ocean_spaces_secret_access_key,
    )
    d.upload_sql_file(
        "./src/schema/backup_files/racehorse-database-schema.sql",
        f"snapshots/{RUNNING_TIME}/racehorse-database-schema.sql",
    )
    table_schema_pairs = [
        ("rp_raw", "performance_data"),
        ("tf_raw", "performance_data"),
    ]
    years = range(2010, datetime.now().year)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(upload_performance_data, schema, table, year, d)
            for year, (schema, table) in itertools.product(years, table_schema_pairs)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                I(f"An error occurred: {e}")

    tables = [
        "horse",
        "jockey",
        "trainer",
        "owner",
        "sire",
        "dam",
        "course",
        "country",
    ]

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(upload_entity_data, table, d) for table in tables]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                I(f"An error occurred: {e}")
