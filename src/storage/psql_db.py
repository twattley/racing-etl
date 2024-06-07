from dataclasses import dataclass
from typing import List, Optional, Union

import pandas as pd
import sqlalchemy

from src.config import config
from src.utils.logging_config import I
from src.utils.time_utils import get_uk_time_now


@dataclass
class PsqlConnection:
    user: str
    password: str
    host: str
    port: int
    db: str


class SQLDatabase:
    connection: PsqlConnection

    def __init__(self, connection: PsqlConnection):
        self.connection = connection

    def storage_connection(self):
        for i in [
            ("user", self.connection.user),
            ("password", self.connection.password),
            ("host", self.connection.host),
            ("port", self.connection.port),
            ("db", self.connection.db),
        ]:
            if not i[1]:
                raise ValueError(f"Missing database connection parameter: {i[0]} ")

        return sqlalchemy.create_engine(
            f"postgresql://{self.connection.user}:{self.connection.password}@{self.connection.host}:{self.connection.port}/{self.connection.db}"
        )

    def store_data(
        self,
        data: pd.DataFrame,
        table: str,
        schema: str,
        truncate: bool = False,
        created_at: bool = False,
    ):
        if data.empty:
            I(f"No data to store in {schema}.{table}")
            return
        with self.storage_connection().begin() as conn:
            if truncate:
                I(f"Truncating {schema}.{table}")
                self.execute_query(f"TRUNCATE TABLE {schema}.{table}")
            if created_at:
                self = data.assign(created_at=get_uk_time_now())
            I(f"Storing {len(data)} records in {schema}.{table}")
            data.to_sql(
                name=table,
                con=conn,
                schema=schema,
                if_exists="append",
                index=False,
            )

    def fetch_data(
        self,
        query: str,
    ) -> pd.DataFrame:
        query = sqlalchemy.text(query)

        I(f"Fetching data with query: {query}")

        with self.storage_connection().begin() as conn:
            df = pd.read_sql(
                query,
                conn,
            )

        return df

    def call_procedure(self, procedure: str, schema: Optional[str] = None):
        full_procedure_name = f"{schema}.{procedure}" if schema else procedure
        I(f"Calling stored procedure {full_procedure_name}()")

        connection = self.storage_connection().raw_connection()
        cursor = connection.cursor()

        cursor.execute(f"call {full_procedure_name}();")
        connection.commit()  # wait for transaction to complete

        cursor.close()
        connection.close()

        I(f"Stored procedure {full_procedure_name}() completed")

    def execute_query(self, query: str):
        I(f"Executing query: {query}")

        with self.storage_connection().begin() as conn:
            result = conn.execute(sqlalchemy.text(query))
            affected_rows = result.rowcount

        I(f"Query executed. Number of rows affected: {affected_rows}")

    def select_function(
        self, schema: str, function: str, args: list = None
    ) -> pd.DataFrame:
        I(f"Calling function {schema}.{function}()")

        connection = self.storage_connection().raw_connection()
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

        return pd.DataFrame(result, columns=columns)

    def delete_duplicates_from_table(self, schema: str, table: str, unique_id: str):
        I(f"Deleting duplicates from {schema}.{table}")

        with self.storage_connection().begin() as conn:
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
        self,
        table: str,
        schema: str,
        data: pd.DataFrame,
        unique_id: Union[str, List[str]],
    ):
        current_data = self.fetch_data(f"SELECT * FROM {schema}.{table}")
        data = (
            data.sort_values(by="created_at", ascending=False)
            .drop_duplicates(subset=unique_id, keep="first")
            .reset_index(drop=True)
        )
        self.store_data(
            data[~data[unique_id].isin(current_data[unique_id])], table, schema
        )

    def insert_records(
        self,
        table: str,
        schema: str,
        data: pd.DataFrame,
        unique_id: Union[str, List[str]],
    ):
        current_data = self.fetch_data(f"SELECT * FROM {schema}.{table}")
        data = data.drop_duplicates(subset=unique_id).reset_index(drop=True)
        self.store_data(
            data[~data[unique_id].isin(current_data[unique_id])], table, schema
        )


def get_db():
    return SQLDatabase(
        PsqlConnection(
            user=config.pg_db_user,
            password=config.pg_db_password,
            host=config.pg_db_host,
            port=config.pg_db_port,
            db=config.pg_db_name,
        )
    )
