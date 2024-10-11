from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Timeform(BaseSettings):
    email: str
    password: str
    login_url: str

    model_config = SettingsConfigDict(env_prefix="TF__")


class Betfair(BaseSettings):
    username: str
    password: str
    app_key: str
    certs_path: str
    historical_data_path: str

    model_config = SettingsConfigDict(env_prefix="BF__")


class Postgres(BaseSettings):
    host: str
    user: str
    name: str
    password: str
    port: int

    model_config = SettingsConfigDict(env_prefix="PG__")


class S3(BaseSettings):
    access_key: str
    secret_access_key: str
    region_name: str
    endpoint_url: str
    bucket_name: str

    model_config = SettingsConfigDict(env_prefix="S3__")


class Config(BaseSettings):
    runtime_environment: Literal["CLOUD", "LOCAL"] = "LOCAL"
    chromedriver_path: str
    tf: Timeform
    bf: Betfair
    pg: Postgres
    s3: S3

    model_config = SettingsConfigDict(env_nested_delimiter="__")
