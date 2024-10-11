from typing import Literal

from api_helpers.clients.postgres_client import PostgresClient, PsqlConnection
from api_helpers.clients.s3_client import S3Client, S3Connection
from api_helpers.interfaces.storage_client_interface import IStorageClient

from src.config import Config


def get_storage_client(
    client_type: Literal["postgres", "s3"],
) -> IStorageClient:
    config = Config()
    if client_type == "postgres":
        return PostgresClient(
            PsqlConnection(
                user=config.pg.user,
                password=config.pg.password,
                host=config.pg.host,
                port=config.pg.port,
                db=config.pg.name,
            )
        )
    elif client_type == "s3":
        return S3Client(
            S3Connection(
                access_key_id=config.s3.access_key,
                secret_access_key=config.s3.secret_access_key,
                region_name=config.s3.region_name,
                endpoint_url=config.s3.endpoint_url,
                bucket_name=config.s3.bucket_name,
            )
        )
