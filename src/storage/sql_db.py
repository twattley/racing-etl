import os
from typing import List, Optional, Union

import pandas as pd
import sqlalchemy

from src.utils.logging_config import I
from src.utils.time_utils import get_uk_time_now


def storage_connection(
    user: str = None,
    password: str = None,
    host: str = None,
    port: int = None,
    db: str = None,
):
    user = user or os.environ.get("PG_DB_USER")
    password = password or os.environ.get("PG_DB_PASSWORD")
    host = host or os.environ.get("PG_DB_HOST")
    port = port or os.environ.get("PG_DB_PORT")
    db = db or os.environ.get("PG_DB_NAME")

    for i in [
        ("user", user),
        ("password", password),
        ("host", host),
        ("port", port),
        ("db", db),
    ]:
        if not i[1]:
            raise ValueError(f"Missing database connection parameter: {i[0]} ")

    return sqlalchemy.create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    )


def store_data(
    data: pd.DataFrame,
    table: str,
    schema: str,
    truncate: bool = False,
    created_at: bool = False,
):
    if data.empty:
        I(f"No data to store in {schema}.{table}")
        return
    with storage_connection().begin() as conn:
        if truncate:
            I(f"Truncating {schema}.{table}")
            execute_query(f"TRUNCATE TABLE {schema}.{table}")
        if created_at:
            data = data.assign(created_at=get_uk_time_now())
        I(f"Storing {len(data)} records in {schema}.{table}")
        data.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
        )


def fetch_data(
    query: str,
) -> pd.DataFrame:
    query = sqlalchemy.text(query)

    I(f"Fetching data with query: {query}")

    with storage_connection().begin() as conn:
        df = pd.read_sql(
            query,
            conn,
        )

    return df


def call_procedure(procedure: str, schema: Optional[str] = None):
    full_procedure_name = f"{schema}.{procedure}" if schema else procedure
    I(f"Calling stored procedure {full_procedure_name}()")

    connection = storage_connection().raw_connection()
    cursor = connection.cursor()

    cursor.execute(f"call {full_procedure_name}();")
    connection.commit()  # wait for transaction to complete

    cursor.close()
    connection.close()

    I(f"Stored procedure {full_procedure_name}() completed")


def execute_query(query: str):
    I(f"Executing query: {query}")

    with storage_connection().begin() as conn:
        result = conn.execute(sqlalchemy.text(query))
        affected_rows = result.rowcount

    I(f"Query executed. Number of rows affected: {affected_rows}")


def select_function(schema: str, function: str, args: list = None) -> pd.DataFrame:
    I(f"Calling function {schema}.{function}()")

    connection = storage_connection().raw_connection()
    cursor = connection.cursor()

    if args:
        stmt = f"SELECT * FROM {schema}.{function}({','.join(map(str, args))});"
    else:
        stmt = f"SELECT * FROM {schema}.{function}();"

    I(stmt)

    cursor.execute(stmt)
    result = cursor.fetchall()
    
    columns = [desc[0] for desc in cursor.description]

    cursor.close()
    connection.close()

    I(f"Function {schema}.{function}() completed")

    df = pd.DataFrame(result, columns=columns)

    return df


def delete_duplicates_from_table(schema: str, table: str, unique_id: str):
    I(f"Deleting duplicates from {schema}.{table}")

    with storage_connection().begin() as conn:
        conn.execute(
            f"""
            CREATE TEMP TABLE temp_unique AS
            SELECT DISTINCT ON ({unique_id}) *
            FROM {schema}.{table};

            DELETE FROM {schema}.{table};

            INSERT INTO {schema}.{table}
            SELECT * FROM temp_unique;

            DROP TABLE temp_unique;

            """
        )
    I(f"Duplicates deleted from {schema}.{table}")


def insert_latest_records(
    table: str, schema: str, data: pd.DataFrame, unique_id: Union[str, List[str]]
):
    current_data = fetch_data(f"SELECT * FROM {schema}.{table}")
    data = (
        data.sort_values(by="created_at", ascending=False)
        .drop_duplicates(subset=unique_id, keep="first")
        .reset_index(drop=True)
    )
    store_data(data[~data[unique_id].isin(current_data[unique_id])], table, schema)


def insert_records(
    table: str, schema: str, data: pd.DataFrame, unique_id: Union[str, List[str]]
):
    current_data = fetch_data(f"SELECT * FROM {schema}.{table}")
    data = data.drop_duplicates(subset=unique_id).reset_index(drop=True)
    store_data(data[~data[unique_id].isin(current_data[unique_id])], table, schema)
