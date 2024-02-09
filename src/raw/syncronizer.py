import os
import time
from src.storage.sql_db import fetch_data


class Syncronizer:
    def __init__(self, job_name):
        self.job_name = job_name

    def set_database(self):
        if self.job_name == "rp_scrape_links":
            self.database_schema = "rp_raw"
            self.database_table = "missing_dates"
        elif self.job_name == "rp_scrape_data":
            self.database_schema = "rp_raw"
            self.database_table = "missing_links"
        elif self.job_name == "tf_scrape_links":
            self.database_schema = "tf_raw"
            self.database_table = "days_results_links"
        elif self.job_name == "tf_scrape_data":
            self.database_schema = "tf_raw"
            self.database_table = "missing_links"

        if self.job_name.startswith("tf"):
            self.log_file = os.path.join(
                os.getcwd(), "src/raw/timeform", f"{self.job_name}.csv"
            )
        else:
            self.log_file = os.path.join(
                os.getcwd(), "src/raw/racing-post", f"{self.job_name}.csv"
            )

    def pull_data(self):
        self.data = fetch_data(
            f"SELECT * FROM {self.database_schema}.{self.database_table}"
        )

    def save_data(self):
        self.data.to_csv(self.log_file, index=False)

    def sync(self):
        self.pull_data()
        self.save_data()
        return self.data


def run_sync(job_name):
    s = Syncronizer(job_name)
    s.set_database()
    while True:
        s.sync()
        time.sleep(180)


def sync(job_name):
    s = Syncronizer(job_name)
    s.set_database()
    s.sync()


if __name__ == "__main__":
    import sys

    job_name = sys.argv[1]
    run_sync(job_name)
