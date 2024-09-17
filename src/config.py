import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

TF_EMAIL = "wattley83@gmail.com"
TF_PASSWORD = "Watford_1"
BF_USERNAME = "trialaccount83"
BF_PASSWORD = "q4Nnt#n*fbgs-dT"
BF_APP_KEY = "l5XwEV3zntgcJfQz"
BF_CERTS_PATH = "/Users/tomwattley/.betfair/certs"
BETFAIR_HISTORICAL_DATA_PATH = "./data/betfair"
PG_DB_HOST = "localhost"
PG_DB_NAME = "racehorse-database"
PG_DB_USER = "postgres"
PG_DB_PASSWORD = "postgres"
PG_DB_PORT = 5432
DO_SPACES_ACCESS_KEY_ID = "DO00ZRJGBDDABCU4VHRH"
DO_SPACES_SECRET_ACCESS_KEY = "32JDnCwlWEl0JsUmq1bhh6njY1DmR3VtHR3TqgTeCf8"
CHROMEDRIVER_PATH = "/Users/tomwattley/chromedriver/chromedriver"


class Config(BaseSettings):
    tf_email: str = TF_EMAIL
    tf_password: str = TF_PASSWORD
    bf_username: str = BF_USERNAME
    bf_password: str = BF_PASSWORD
    bf_app_key: str = BF_APP_KEY
    bf_certs_path: str = "/Users/tomwattley/.betfair/certs"
    bf_historical_data_path: str = "./data/betfair"
    pg_db_host: str = "localhost"
    pg_db_user: str = "postgres"
    pg_db_name: str = "racehorse-database"
    pg_db_password: str = PG_DB_PASSWORD
    pg_db_port: int = 5432
    chromedriver_path: str = CHROMEDRIVER_PATH


def load_config():
    env = os.environ.get("ENV", "DEV")
    if env == "DEV":
        env_file = ".env"
    elif env == "TEST":
        env_file = "./tests/.test.env"
    load_dotenv(env_file, override=True)
    return Config()


config = load_config()
