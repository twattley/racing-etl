from api_helpers.interfaces.storage_client_interface import IStorageClient
from src.entity_matching.timeform.entity_matcher import TimeformEntityMatcher
from src.entity_matching.betfair.entity_matcher import BetfairEntityMatcher
from src.storage.storage_client import get_storage_client


def run_matching_pipeline(storage_client: IStorageClient):
    tf_entity_matcher = TimeformEntityMatcher(storage_client)
    betfair_entity_matcher = BetfairEntityMatcher(storage_client)

    tf_entity_matcher.run_matching()
    betfair_entity_matcher.run_matching()


if __name__ == "__main__":
    storage_client = get_storage_client("postgres")
    run_matching_pipeline(storage_client)
