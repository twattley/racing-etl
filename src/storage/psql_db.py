from api_helpers.db_client import PsqlConnection, SQLDatabase

from src.config import config


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
