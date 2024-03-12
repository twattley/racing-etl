from src.raw.racing_post.scrape_data import process_rp_scrape_data
from src.raw.racing_post.scrape_links import process_rp_scrape_links
from src.raw.timeform.scrape_data import process_tf_scrape_data
from src.raw.timeform.scrape_links import process_tf_scrape_links


def run_scraping_pipeline():
    process_rp_scrape_links()
    process_tf_scrape_links()
    process_rp_scrape_data()
    process_tf_scrape_data()


if __name__ == "__main__":
    run_scraping_pipeline()
