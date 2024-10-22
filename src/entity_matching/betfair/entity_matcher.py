from api_helpers.interfaces.storage_client_interface import IStorageClient
import pandas as pd
from api_helpers.helpers.logging_config import W, I
from api_helpers.helpers.processing_utils import ptr
from src.storage.storage_client import get_storage_client
from src.entity_matching.helpers.string_formatting import format_horse_name
from src.entity_matching.interfaces.entity_matching_interface import IEntityMatching


class BetfairEntityMatcher(IEntityMatching):
    def __init__(self, storage_client: IStorageClient):
        self.storage_client = storage_client

    def run_matching(self):
        rp_data, tf_data = self.fetch_data()
        if rp_data.empty:
            I("No RP data to match")
            return
        matched_data = self.match_data(rp_data, tf_data)
        if matched_data.empty:
            W("No data matched")
            return
        entity_data = self.create_entity_data(matched_data)
        self.store_data(entity_data)

    def fetch_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        return ptr(
            lambda: self.storage_client.fetch_data(
                "SELECT * FROM entities.matching_todays_rp_entities"
            ),
            lambda: self.storage_client.fetch_data(
                "SELECT * FROM entities.matching_todays_bf_entities"
            ),
        )

    def store_data(self, entity_data: pd.DataFrame):
        self.storage_client.upsert_data(
            entity_data,
            "entities",
            "horse",
            ["horse_id", "bf_horse_id"],
        )

    def create_entity_data(self, data: pd.DataFrame) -> list[dict[str, pd.DataFrame]]:
        entity_data = data[
            [
                "horse_id_x",
                "horse_id_y",
            ]
        ].drop_duplicates()
        return entity_data.rename(
            columns={
                "horse_id_x": "horse_id",
                "horse_id_y": "bf_horse_id",
            }
        )

    def match_data(self, bf_data: pd.DataFrame, rp_data: pd.DataFrame) -> pd.DataFrame:
        I(f"Matching {rp_data.shape[0]} RP horses")
        rp_data = rp_data.pipe(format_horse_name)
        bf_data = bf_data.pipe(format_horse_name)
        unmatched_data = 0
        matched_data_rows = []
        for i in rp_data.itertuples():
            bf_matching_data = bf_data[
                (bf_data["race_date"] == i.race_date)
                & (bf_data["course_id"] == i.course_id)
                & (bf_data["filtered_horse_name"] == i.filtered_horse_name)
            ]
            if not bf_matching_data.empty:
                one_row_df = pd.DataFrame.from_records([i._asdict()])
                matched_data = one_row_df.merge(
                    bf_matching_data,
                    on=["race_date", "course_id", "filtered_horse_name"],
                    how="left",
                )
                matched_data_rows.append(matched_data)
            else:
                unmatched_data += 1

        W(f"Number of unmatched rows: {unmatched_data}")
        if len(matched_data_rows) > 0:
            return pd.concat(matched_data_rows)
        else:
            return pd.DataFrame()


if __name__ == "__main__":
    service = BetfairEntityMatcher(get_storage_client("postgres"))
    service.run_matching()
