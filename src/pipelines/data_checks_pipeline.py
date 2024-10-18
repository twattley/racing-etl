from api_helpers.helpers.logging_config import E, I
from api_helpers.helpers.processing_utils import ptr

from src.data_quality.raw.todays_data_quality import TodaysDataQuality
from src.storage.storage_client import PostgresClient, get_storage_client

db: PostgresClient = get_storage_client("postgres")

rp_data_quality = TodaysDataQuality(
    postgres_client=get_storage_client("postgres"),
    schema="rp_raw",
    runtime_environment=config.runtime_environment,
)
tf_data_quality = TodaysDataQuality(
    postgres_client=get_storage_client("postgres"),
    schema="tf_raw",
    runtime_environment=config.runtime_environment,
)


def run_data_checks_pipeline():
    missing_records_between_sets, missing_rp_data_in_final = ptr(
        lambda: db.fetch_data("SELECT * FROM metrics.record_count_differences_vw"),
        lambda: db.fetch_data("SELECT * FROM metrics.missing_raw_data"),
    )

    if missing_records_between_sets.empty:
        I("No missing data between RP and TF")
    else:
        for record in missing_records_between_sets.itertuples():
            E(
                f"MISSING DATA:\n {record.course} {record.race_date} {record.rp_num_records} {record.tf_num_records}"
            )

    if missing_rp_data_in_final.empty:
        I("No missing data in final performance data table")
    else:
        for record in missing_rp_data_in_final.itertuples():
            E(f"MISSING RP DATA IN FINAL: {record.race_timestamp} {record.horse_name}")


if __name__ == "__main__":
    run_data_checks_pipeline()
