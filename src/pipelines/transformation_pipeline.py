from src.transform.data_transformer_service import DataTransformation
from src.storage.storage_client import get_storage_client


def run_transformation_pipeline():
    transformation_service = DataTransformation(
        storage_client=get_storage_client("postgres")
    )
    transformation_service.transform_results_data()
    # transformation_service.transform_todays_data()


if __name__ == "__main__":
    run_transformation_pipeline()
