from dataclasses import dataclass, fields
from datetime import date, datetime
from src.utils.logging_config import I

import pandas as pd


@dataclass
class BaseDataModel: ...


def convert_data(
    data: pd.DataFrame,
    data_model: BaseDataModel,
) -> pd.DataFrame:

    for field in fields(data_model):
        if field.type == datetime:
            data[field.name] = pd.to_datetime(data[field.name], errors="coerce")
        if field.type == date:
            data[field.name] = pd.to_datetime(data[field.name], errors="coerce").dt.date
        elif field.type == int:
            data[field.name] = pd.to_numeric(data[field.name], errors="coerce").astype(
                "Int64"
            )
        elif field.type == float:
            data[field.name] = pd.to_numeric(data[field.name], errors="coerce")

    return data


def validate_data(
    data: pd.DataFrame,
    data_model: BaseDataModel,
) -> pd.DataFrame:

    data_model_fields = {field.name for field in fields(data_model)}
    missing_columns = ", ".join(data_model_fields - set(data.columns))
    if missing_columns:
        raise ValueError(
            f"Data does not contain all required columns, missing: {missing_columns}"
        )

    extra_columns = ", ".join(set(data.columns) - data_model_fields)
    if extra_columns:
        raise ValueError(f"Data contains extra columns: {extra_columns}")

    if sorted(data.columns) != sorted([field.name for field in fields(data_model)]):
        raise ValueError("Data missing columns or has extra columns.")

    return data


def sort_data(
    data: pd.DataFrame, data_model: BaseDataModel, unique_id: str
) -> pd.DataFrame:
    data = data.sort_values(by="created_at", ascending=False).drop_duplicates(
        subset=[unique_id]
    )[[field.name for field in fields(data_model)]]
    I(f"Data Sample {data.head(10)}")
    return data


def convert_and_validate_data(
    data: pd.DataFrame,
    data_model: BaseDataModel,
    unique_id: str,
) -> pd.DataFrame:

    return (
        data.pipe(convert_data, data_model)
        .pipe(validate_data, data_model)
        .pipe(sort_data, data_model, unique_id)
    )
