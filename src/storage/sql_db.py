import os
from datetime import datetime
from typing import Optional

import pandas as pd
import sqlalchemy

from src.utils.logging_config import I


def storage_connection():
    return sqlalchemy.create_engine(os.environ["CONN_STR"])


def store_data(
    data: pd.DataFrame,
    table: str,
    schema: str,
    truncate: bool = False,
    created_at: bool = False,
    extra_query: list = None,
    test: bool = False,
    replace: bool = False,
    print_data: bool = False,
):
    exists_logic = "replace" if replace else "append"
    if print_data:
        print(data)
    if test:
        data.to_csv(f"~/Desktop/{schema}_{table}.csv", index=False)
        return
    if data.empty:
        I(f"No data to store in {schema}.{table}")
        return
    with storage_connection().begin() as conn:
        if truncate:
            I(f"Truncating {schema}.{table}")
            execute_query(f"TRUNCATE TABLE {schema}.{table}")
        if created_at:
            data = data.copy()
            data["created_at"] = datetime.now()
        if extra_query:
            for query in extra_query:
                execute_query(query)
        I(f"Storing {len(data)} records in {schema}.{table}")
        data.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists=exists_logic,
            index=False,
        )


def fetch_data(
    query: str,
    extra_args: dict = None,
    extra_query: str = None,
    without_logging: bool = False,
) -> pd.DataFrame:
    if extra_args:
        query = sqlalchemy.text(f"{query} " + extra_args)
        I(f"Fetching data from query: {query}")
    else:
        query = sqlalchemy.text(query)

    with storage_connection().begin() as conn:
        df = pd.read_sql(
            query,
            conn,
        )
        if extra_query:
            conn.execute(sqlalchemy.text(extra_query))
        if not without_logging:
            I(f"Query {query} \n\t\t | returned {len(df)} records |")
    return df


def upsert_data(
    new_data: pd.DataFrame,
    existing_data: pd.DataFrame,
    table: str,
    schema: str,
    duplicate_keys: list,
):

    if "created_at" not in new_data.columns:
        new_data = new_data.assign(created_at=datetime.now())

    if not existing_data.empty:
        print("existing_data")
        print(existing_data)
        print(existing_data.columns)

        print("new_data")
        print(new_data)
        print(new_data.columns)
        new_dataset = pd.concat(
            [new_data, existing_data], ignore_index=True
        ).sort_values(by="created_at")
    else:
        new_dataset = new_data
    new_dataset = new_dataset.drop_duplicates(subset=duplicate_keys, keep="last")

    store_data(
        new_dataset,
        table,
        schema,
        truncate=True,
        created_at=True,
    )


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
        stmt = f"select {schema}.{function}({','.join(args)});"
    else:
        stmt = f"select {schema}.{function}();"

    I(stmt)

    cursor.execute(stmt)
    result = cursor.fetchall()

    cursor.close()
    connection.commit()
    connection.close()

    I(f"Function {schema}.{function}() completed")

    return result


def update_ref_table(entity: str, matched_entities: pd.DataFrame) -> None:
    if matched_entities.empty:
        I(f"No data to update in {entity}")
        return
    store_data(matched_entities, f"{entity}", "temp")
    execute_query(
        f"""
            UPDATE public.{entity}
            SET tf_{entity}_id = es.tf_{entity}_id
            FROM temp.{entity} es
            WHERE public.{entity}.rp_{entity}_id = es.rp_{entity}_id;
    """
    )
    execute_query(f"DROP TABLE temp.{entity}")
