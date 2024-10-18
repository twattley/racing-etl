from src.config import Config

from api_helpers.clients.betfair_client import BetFairClient, BetfairCredentials
from src.raw.racing_post.ingestor import RPIngestor
from src.raw.timeform.ingestor import TFIngestor
from src.raw.betfair.ingestor import BFIngestor
from src.storage.storage_client import get_storage_client


def run_ingestion_pipeline():
    config = Config()
    betfair_client = BetFairClient(
        BetfairCredentials(
            username=config.bf_username,
            password=config.bf_password,
            app_key=config.bf_app_key,
            certs_path=config.bf_certs_path,
        )
    )
    storage_client = get_storage_client("postgres")

    rp_ingestor = RPIngestor(config=config, storage_client=storage_client)
    tf_ingestor = TFIngestor(config=config, storage_client=storage_client)
    bf_ingestor = BFIngestor(
        config=config, storage_client=storage_client, betfair_client=betfair_client
    )

    # rp_ingestor.ingest_results_links()
    # tf_ingestor.ingest_results_links()

    # rp_ingestor.ingest_todays_links()
    # tf_ingestor.ingest_todays_links()

    # rp_ingestor.ingest_todays_data()
    # tf_ingestor.ingest_todays_data()

    # rp_ingestor.ingest_results_data()
    # tf_ingestor.ingest_results_data()

    # rp_ingestor.ingest_results_data_world()
    # tf_ingestor.ingest_results_data_world()

    # bf_ingestor.ingest_todays_data()
    bf_ingestor.ingest_results_data()


if __name__ == "__main__":
    run_ingestion_pipeline()
