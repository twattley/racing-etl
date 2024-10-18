from dataclasses import dataclass
from typing import Literal

from api_helpers.clients.betfair_client import BetFairClient, BetfairCredentials
from api_helpers.clients.postgres_client import PostgresClient
from api_helpers.clients.s3_client import S3Client
from api_helpers.helpers.logging_config import I
from api_helpers.helpers.processing_utils import pt

from src.config import Config
from src.raw.daos.postgres_dao import PostgresDao
from src.raw.daos.s3_dao import S3Dao
from src.storage.storage_client import get_storage_client

from src.raw.betfair.ingestor import BFIngestor
from src.raw.racing_post.ingestor import RPIngestor
from src.raw.timeform.ingestor import TFIngestor


@dataclass
class Views:
    results_data: str
    results_data_world: str


def ingest_data_from_s3(
    schema: str, table: str, storage_type: Literal["upsert", "insert"]
):
    s3_client: S3Client = get_storage_client("s3")
    postgres_client: PostgresClient = get_storage_client("postgres")

    data = s3_client.fetch_data(f"{schema}/{table}.parquet")
    I(f"Ingested {len(data)} rows from {schema}.{table}")

    if storage_type == "upsert":
        I(f"Upserting {len(data)} rows into {schema}.{table}")
        postgres_client.upsert_data(data, table, schema, unique_columns=["unique_id"])
    elif storage_type == "insert":
        I(f"Inserting {len(data)} rows into {schema}.{table}")
        postgres_client.store_data(data, table, schema, truncate=True)


def ingest_raw_performance_data_from_s3(environment: Literal["LOCAL", "CLOUD"]):
    if environment == "CLOUD":
        I("Skipping ingestion of raw performance data in cloud")
        return

    # RACING POST
    pt(
        lambda: ingest_data_from_s3(
            schema="rp_raw",
            table="results_data",
            storage_type="upsert",
        ),
        lambda: ingest_data_from_s3(
            schema="rp_raw",
            table="results_data_world",
            storage_type="upsert",
        ),
    )
    pt(
        lambda: ingest_data_from_s3(
            schema="rp_raw",
            table="todays_data",
            storage_type="insert",
        ),
        lambda: ingest_data_from_s3(
            schema="rp_raw",
            table="todays_links",
            storage_type="insert",
        ),
    )

    # TIMEFORM
    pt(
        lambda: ingest_data_from_s3(
            schema="tf_raw",
            table="performance_data",
            storage_type="upsert",
        ),
        lambda: ingest_data_from_s3(
            schema="tf_raw",
            table="results_data_world",
            storage_type="upsert",
        ),
    )
    pt(
        lambda: ingest_data_from_s3(
            schema="tf_raw",
            table="todays_data",
            storage_type="insert",
        ),
        lambda: ingest_data_from_s3(
            schema="tf_raw",
            table="todays_links",
            storage_type="insert",
        ),
    )
    # BETFAIR
    pt(
        lambda: ingest_data_from_s3(
            schema="bf_raw",
            table="historical_price_data",
            storage_type="upsert",
        ),
        lambda: ingest_data_from_s3(
            schema="bf_raw",
            table="todays_data",
            storage_type="insert",
        ),
    )


def run_ingestion_pipeline():
    config = Config()
    dao_map = {
        "CLOUD": S3Dao(),
        "LOCAL": PostgresDao(),
    }
    dao = dao_map[config.runtime_environment]
    betfair_client = BetFairClient(
        BetfairCredentials(
            username=config.bf_username,
            password=config.bf_password,
            app_key=config.bf_app_key,
            certs_path=config.bf_certs_path,
        )
    )

    rp_ingestor = RPIngestor(config=config, dao=dao)
    tf_ingestor = TFIngestor(config=config, dao=dao)
    bf_ingestor = BFIngestor(config=config, dao=dao, betfair_client=betfair_client)

    # ingest_raw_performance_data_from_s3(environment=config.runtime_environment)

    rp_ingestor.ingest_results_links()
    tf_ingestor.ingest_results_links()

    # Racecards Links
    rp_ingestor.ingest_todays_links()
    tf_ingestor.ingest_todays_links()

    # Racecards Data
    rp_ingestor.ingest_todays_data()
    tf_ingestor.ingest_todays_data()

    # UK/IRE Results Data
    rp_ingestor.ingest_results_data()
    tf_ingestor.ingest_results_data()

    # World Results Data
    rp_ingestor.ingest_results_data_world()
    tf_ingestor.ingest_results_data_world()

    # Betfair Data
    bf_ingestor.ingest_todays_data()
    bf_ingestor.ingest_results_data()

    # rp_data_quality.check_data_quality()
    # tf_data_quality.check_data_
    #
    #
    # quality()


if __name__ == "__main__":
    run_ingestion_pipeline()
