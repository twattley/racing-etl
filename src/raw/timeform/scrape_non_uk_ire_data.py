from src.raw.data_types import DatabaseInfo
from src.raw.timeform.scrape_historical_data import process_tf_scrape_data
from src.utils.logging_config import I


def process_tf_scrape_non_uk_ire_data():
    I("Processing non-UK/IRE data from Timeform")
    db_info = DatabaseInfo(
        schema="tf_raw",
        job_name="process_tf_scrape_non_uk_ire_data",
        source_view="missing_non_uk_ire_links",
        dest_table="non_uk_ire_performance_data",
    )
    process_tf_scrape_data(db_info)


if __name__ == "__main__":
    process_tf_scrape_non_uk_ire_data()
