from typing import Literal

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv(override=True)


class Config(BaseSettings):
    runtime_environment: Literal["CLOUD", "LOCAL"] = "LOCAL"
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

    # Ingestion
    results_links_table_name: str
    results_links_view_name: str

    racecards_links_table_name: str
    racecards_links_view_name: str

    results_data_table_name: str
    results_data_view_name: str

    racecards_data_table_name: str
    racecards_data_view_name: str

    ingestion_base_table_name: str
    ingestion_base_view_name: str
