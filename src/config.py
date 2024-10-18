from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv(dotenv_path=".env", override=True)


class RawSchema(BaseSettings):
    results_links_table: str = "results_links"
    results_links_view: str = "missing_results_links"

    racecards_links_table: str = "todays_links"
    racecards_links_view: str = "missing_todays_links"

    results_data_table: str = "results_data"
    results_data_view: str = "results_data_view"

    results_data_table_world: str = "results_data_world"
    results_data_view_world: str = "results_data_view_world"

    todays_data_table: str = "todays_data"


class DB(BaseSettings):
    raw: RawSchema = RawSchema()


class Config(BaseSettings):
    chromedriver_path: str

    bf_username: str
    bf_password: str
    bf_app_key: str
    bf_certs_path: str
    bf_historical_data_path: str

    pg_host: str
    pg_user: str
    pg_name: str
    pg_password: str
    pg_port: int

    tf_email: str
    tf_password: str
    tf_login_url: str

    s3_access_key: str
    s3_secret_access_key: str
    s3_region_name: str
    s3_endpoint_url: str
    s3_bucket_name: str

    db: DB = DB()
