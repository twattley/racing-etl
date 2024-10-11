from api_helpers.helpers.logging_config import I, W
from api_helpers.helpers.processing_utils import pt, ptr

from src.data_models.transform.race_model import RaceDataModel
from src.data_models.transform.transformed_model import TransformedDataModel
from src.storage.storage_client import get_storage_client
from src.transform.transform_data import transform_data

db = get_storage_client("postgres")


class TransformationPipeline:
    def process_data(
        self,
        data_type,
        view_name,
        transformed_table,
        race_table,
        rejected_table,
    ):
        data = db.fetch_data(f"SELECT * FROM public.{view_name};")
        if data.empty:
            I(f"No missing {data_type} data to transform.")
            return

        accepted_data, rejected_data, race_data = transform_data(
            data=data,
            transform_data_model=TransformedDataModel,
            race_data_model=RaceDataModel,
            data_type=data_type,
        )
        pt(
            lambda: db.store_data(
                accepted_data,
                transformed_table,
                "staging",
                truncate=True,
            ),
            lambda: db.store_data(
                rejected_data,
                f"staging_{rejected_table}",
                "errors",
                truncate=True,
            ),
            lambda: db.store_data(race_data, race_table, "staging", truncate=True),
        )

    def process_results_data(self):
        self.process_data(
            data_type="results",
            view_name="missing_performance_data_vw",
            transformed_table="transformed_performance_data",
            race_table="transformed_race_data",
            rejected_table="transformed_performance_data_rejected",
        )

    def process_non_uk_ire_results_data(self):
        self.process_data(
            data_type="non_uk_ire_results",
            view_name="missing_non_uk_ire_performance_data_vw",
            transformed_table="transformed_non_uk_ire_performance_data",
            race_table="transformed_non_uk_ire_race_data",
            rejected_table="transformed_non_uk_ire_performance_data_rejected",
        )

    def process_todays_data(self):
        self.process_data(
            data_type="todays",
            view_name="missing_todays_performance_data_vw",
            transformed_table="todays_transformed_performance_data",
            race_table="todays_transformed_race_data",
            rejected_table="todays_transformed_performance_data_rejected",
        )

    def post_transform_today_checks(self):
        I("Running post-transform checks")
        todays_staging, todays_data = ptr(
            lambda: db.fetch_data(
                "SELECT DISTINCT unique_id FROM staging.todays_joined_performance_data;"
            ),
            lambda: db.fetch_data(
                "SELECT DISTINCT unique_id FROM public.todays_performance_data;"
            ),
        )

        number_of_staging_records = len(todays_staging)
        number_of_data_records = len(todays_data)

        if number_of_staging_records != number_of_data_records:
            W(
                f"MISSING DATA STAGING: {number_of_staging_records} DATA: {number_of_data_records}"
            )
        else:
            I(
                f"SUCCESS: STAGING: {number_of_staging_records} DATA: {number_of_data_records}"
            )


def run_transformation_pipeline(pipeline: str = None):
    transformer = TransformationPipeline()
    if pipeline == "results":
        transformer.process_results_data()
    elif pipeline == "non_uk_ire_results":
        transformer.process_non_uk_ire_results_data()
    elif pipeline == "todays":
        transformer.process_todays_data()
    else:
        pt(
            transformer.process_results_data,
            transformer.process_non_uk_ire_results_data,
            transformer.process_todays_data,
        )

    pt(
        lambda: db.call_procedure("insert_transformed_performance_data", "public"),
        lambda: db.call_procedure("insert_transformed_race_data", "public"),
    )
    pt(
        lambda: db.call_procedure(
            "insert_transformed_non_uk_ire_performance_data", "public"
        ),
        lambda: db.call_procedure("insert_transformed_non_uk_ire_race_data", "public"),
    )
    pt(
        lambda: db.call_procedure(
            "insert_todays_transformed_performance_data", "public"
        ),
        lambda: db.call_procedure("insert_todays_transformed_race_data", "public"),
    )

    transformer.post_transform_today_checks()


if __name__ == "__main__":
    run_transformation_pipeline("non_uk_ire_results")
