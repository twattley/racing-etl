import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    tf_email: str  
    tf_password: str 
    bf_username: str 
    bf_password: str 
    bf_app_key: str 
    bf_certs_path: str 
    bf_historical_data_path: str = "./data/betfair"
    pg_db_host: str = "localhost"
    pg_db_user: str = "postgres"
    pg_db_name: str = "racehorse-database"
    pg_db_password: str 
    pg_db_port: int = 5432
    chromedriver_path: str = "./chromedriver/chromedriver"


def load_config():
    env = os.environ.get("ENV", "DEV")
    if env == "DEV":
        env_file = ".env"
    elif env == "TEST":
        env_file = "./tests/.test.env"
    load_dotenv(env_file, override=True)
    return Config()


config = load_config()
