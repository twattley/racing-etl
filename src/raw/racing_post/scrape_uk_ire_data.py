from src.raw import DatabaseInfo
from src.raw.racing_post.scrape_historical_data import process_rp_scrape_data

db_info = DatabaseInfo(
    schema="rp_raw",
    job_name="process_rp_scrape_uk_ire_data",
    source_view="missing_uk_ire_links",
    dest_table="performance_data",
)

if __name__ == "__main__":
    process_rp_scrape_data(db_info)
